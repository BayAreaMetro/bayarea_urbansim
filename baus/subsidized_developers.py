from __future__ import print_function
import sys
import time
import orca
import pandas as pd
import numpy as np
from functools import reduce
from urbansim import accounts
from urbansim_defaults import utils
from six import StringIO
from urbansim.utils import misc
from baus.utils import add_buildings
from urbansim.developer import sqftproforma


#@orca.step()
def subsidized_office_developer(feasibility, coffer, formula, year,
                                add_extra_columns_func, buildings,
                                summary, coffer_acct_name):

    # get the total subsidy for subsidizing office
    total_subsidy = coffer[coffer_acct_name].\
        total_transactions_by_subacct("regional")

    # get the office feasibility frame and sort by profit per sqft
    
    feasibility = feasibility.loc[:, "office"]

    feasibility = feasibility.dropna(subset=["max_profit"])

    feasibility["pda_id"] = feasibility.pda_pba40

    # filter to receiving zone
    feasibility = feasibility.query(formula)

    feasibility["non_residential_sqft"] = \
        feasibility.non_residential_sqft.astype("int")

    feasibility["max_profit_per_sqft"] = feasibility.max_profit / \
        feasibility.non_residential_sqft

    # sorting by our choice metric as we're going to pick our devs
    # in order off the top
    feasibility = feasibility.sort_values(['max_profit_per_sqft'])

    # make parcel_id available
    feasibility = feasibility.reset_index()

    print("%.0f subsidy with %d developments to choose from" % (
        total_subsidy, len(feasibility)))

    devs = []

    for dev_id, d in feasibility.iterrows():

        # the logic here is that we allow each dev to have a "normal"
        # profit per square foot and once it does we guarantee that it
        # gets build - we assume the planning commission for each city
        # enables this to happen.  If a project gets enough profit already
        # we just allow it to compete on the open market - e.g. in the
        # non-subsidized office developer

        NORMAL_PROFIT_PER_SQFT = 70  # assume price is around $700/sqft

        if d.max_profit_per_sqft >= NORMAL_PROFIT_PER_SQFT:
            #  competes in open market
            continue
        else:
            amt = (NORMAL_PROFIT_PER_SQFT - d.max_profit_per_sqft) * \
                d.non_residential_sqft

            if amt > total_subsidy:
                # we don't have enough money to buy yet
                continue

        metadata = {
            "description": "Developing subsidized office building",
            "year": year,
            "non_residential_sqft": d["non_residential_sqft"],
            "juris": d["juris"],
            "tra_id": d["tra_id"],
            "parcel_id": d["parcel_id"],
            "index": dev_id
        }

        coffer[coffer_acct_name].add_transaction(-1*amt, subaccount="regional",
                                                 metadata=metadata)

        total_subsidy -= amt

        devs.append(d)

    if len(devs) == 0:
        return

    # record keeping - add extra columns to match building dataframe
    # add the buidings and demolish old buildings, and add to debug output
    devs = pd.DataFrame(devs, columns=feasibility.columns)

    print("Building {:,} subsidized office sqft in {:,} projects".format(
        devs.non_residential_sqft.sum(), len(devs)))
    print("%.0f subsidy left" %total_subsidy)

    devs["form"] = "office"
    devs = add_extra_columns_func(devs)

    add_buildings(buildings, devs)

    summary.add_parcel_output(devs)


def run_subsidized_developer(feasibility, parcels, buildings, households,
                             acct_settings, settings, account, year,
                             form_to_btype_func, add_extra_columns_func,
                             summary, create_deed_restricted=False,
                             policy_name="Unnamed"):
    """
    The subsidized residential developer model.

    Parameters
    ----------
    feasibility : DataFrame
        A DataFrame that is returned from run_feasibility for a given form
    parcels : DataFrameWrapper
        The standard parcels DataFrameWrapper (mostly just for run_developer)
    buildings : DataFrameWrapper
        The standard buildings DataFrameWrapper (passed to run_developer)
    households : DataFrameWrapper
        The households DataFrameWrapper (passed to run_developer)
    acct_settings : Dict
        A dictionary of settings to parameterize the model.  Needs these keys:
        sending_buildings_subaccount_def - maps buildings to subaccounts
        receiving_buildings_filter - filter for eligible buildings
    settings : Dict
        The overall settings
    account : Account
        The Account object to use for subsidization
    year : int
        The current simulation year (will be added as metadata)
    form_to_btype_func : function
        Passed through to run_developer
    add_extra_columns_func : function
        Passed through to run_developer
    summary : Summary
        Used to add parcel summary information
    create_deed_restricted : bool
        Bool for whether to create deed restricted units with the subsidies
        or not.  The logic at the time of this writing is to keep track of
        partial units so that when partial units sum to greater than a unit,
        that unit will be deed restricted.

    Returns
    -------
    Nothing

    Subsidized residential developer is designed to run before the normal
    residential developer - it will prioritize the developments we're
    subsidizing (although this is not strictly required - running this model
    after the market rate developer will just create a temporarily larger
    supply of units, which will probably create less market rate development in
    the next simulated year)
    the steps for subsidizing are essentially these

    1 run feasibility with only_built set to false so that the feasibility of
        unprofitable units are recorded
    2 temporarily filter to ONLY unprofitable units to check for possible
        subsidized units (normal developer takes care of market-rate units)
    3 compute the number of units in these developments
    4 divide cost by number of units in order to get the subsidy per unit
    5 filter developments to parcels in "receiving zone" similar to the way we
        identified "sending zones"
    6 iterate through subaccounts one at a time as subsidy will be limited
        to available funds in the subaccount (usually by jurisdiction)
    7 sort ascending by subsidy per unit so that we minimize subsidy (but total
        subsidy is equivalent to total building cost)
    8 cumsum the total subsidy in the buildings and locate the development
        where the subsidy is less than or equal to the amount in the account -
        filter to only those buildings (these will likely be built)
    9 pass the results as "feasible" to run_developer - this is sort of a
        boundary case of developer but should run OK
    10 for those developments that get built, make sure to subtract from
        account and keep a record (on the off chance that demand is less than
        the subsidized units, run through the standard code path, although it's
        very unlikely that there would be more subsidized housing than demand)
    """
    # step 2
    feasibility = feasibility.replace([np.inf, -np.inf], np.nan)
    feasibility = feasibility[feasibility.max_profit < 0]

    # step 3
    feasibility['ave_sqft_per_unit'] = parcels.ave_sqft_per_unit
    feasibility['residential_units'] = \
        np.floor(feasibility.residential_sqft / feasibility.ave_sqft_per_unit)

    # step 3B
    # can only add units - don't subtract units - this is an approximation
    # of the calculation that will be used to do this in the developer model
    feasibility = feasibility[
        feasibility.residential_units > feasibility.total_residential_units]

    # step 3C
    # towards the end, because we're about to sort by subsidy per unit, some
    # large projects never get built, because it could be a 100 unit project
    # times a 500k subsidy per unit.  thus we're going to try filtering by
    # the maximum subsidy for a single development here
    feasibility = feasibility[feasibility.max_profit > -50*1000000]

    # step 4
    feasibility['subsidy_per_unit'] = \
        -1 * feasibility['max_profit'] / feasibility['residential_units']
    # assumption that even if the developer says this property is almost
    # profitable, even the administration costs are likely to cost at least
    # 10k / unit
    feasibility['subsidy_per_unit'] = feasibility.subsidy_per_unit.clip(10000)
    
    feasibility["pda_id"] = feasibility.pda_pba40
   
    # step 5
    feasibility = feasibility.\
            query(acct_settings["alternate_buildings_filter"])

    new_buildings_list = []
    sending_bldgs = acct_settings["sending_buildings_subaccount_def"]
    feasibility["regional"] = 1
    feasibility["subaccount"] = feasibility.eval(sending_bldgs)
    # step 6
    for subacct, amount in account.iter_subaccounts():
        print("Subaccount: ", subacct)

        df = feasibility[feasibility.subaccount == subacct]
        print("Number of feasible projects in receiving zone:", len(df))

        if len(df) == 0:
            continue

        # step 7
        df = df.sort_values(['subsidy_per_unit'], ascending=True)
        # df.to_csv('subsidized_units_%d_%s_%s.csv' %
        #           (orca.get_injectable("year"), account.name, subacct))

        # step 8
        print("Amount in subaccount: ${:,.2f}".format(amount))
        num_bldgs = int((-1*df.max_profit).cumsum().searchsorted(amount))

        if num_bldgs == 0:
            continue

        # technically we only build these buildings if there's demand
        # print "Building {:d} subsidized buildings".format(num_bldgs)
        df = df.iloc[:int(num_bldgs)]

        df.columns = pd.MultiIndex.from_tuples(
            [("residential", col) for col in df.columns])
        # disable stdout since developer is a bit verbose for this use case
        sys.stdout, old_stdout = StringIO(), sys.stdout

        kwargs = settings['residential_developer']
        # step 9
        new_buildings = utils.run_developer(
            "residential",
            households,
            buildings,
            "residential_units",
            parcels.parcel_size,
            parcels.ave_sqft_per_unit,
            parcels.total_residential_units,
            orca.DataFrameWrapper("feasibility", df),
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func,
            profit_to_prob_func=profit_to_prob_func,
            **kwargs)
        sys.stdout = old_stdout
        buildings = orca.get_table("buildings")

        if new_buildings is None:
            continue

        # keep track of partial subsidized untis so that we always get credit
        # for a partial unit, even if it's not built in this specific building
        partial_subsidized_units = 0

        # step 10
        for index, new_building in new_buildings.iterrows():

            amt = new_building.max_profit
            metadata = {
                "description": "Developing subsidized building",
                "year": year,
                "residential_units": new_building.residential_units,
                "inclusionary_units": new_building.inclusionary_units,
                "building_id": index
            }

            if create_deed_restricted:

                revenue_per_unit = new_building.building_revenue / \
                    new_building.residential_units
                total_subsidy = abs(new_building.max_profit)
                subsidized_units = total_subsidy / revenue_per_unit + \
                    partial_subsidized_units
                # right now there are inclusionary requirements
                already_subsidized_units = new_building.deed_restricted_units

                # get remainder
                partial_subsidized_units = subsidized_units % 1
                # round off for now
                subsidized_units = int(subsidized_units) + \
                    already_subsidized_units
                # cap at number of residential units
                subsidized_units = min(subsidized_units,
                                       new_building.residential_units)

                buildings.local.loc[index, "deed_restricted_units"] =\
                    int(round(subsidized_units))

                buildings.local.loc[index, "subsidized_units"] = \
                    buildings.local.loc[index, "deed_restricted_units"] - \
                    buildings.local.loc[index, "inclusionary_units"]

                # also correct the debug output
                new_buildings.loc[index, "deed_restricted_units"] = \
                    int(round(subsidized_units))
                new_buildings.loc[index, "subsidized_units"] = \
                    new_buildings.loc[index, "deed_restricted_units"] - \
                    new_buildings.loc[index, "inclusionary_units"]

            metadata['deed_restricted_units'] = \
                new_buildings.loc[index, 'deed_restricted_units']
            metadata['subsidized_units'] = \
                new_buildings.loc[index, 'subsidized_units']
            account.add_transaction(amt, subaccount=subacct,
                                    metadata=metadata)

        # turn off this assertion for the Draft Blueprint
        # affordable housing policy since the number of deed restricted units
        # vs units from development projects looks reasonable
#        assert np.all(buildings.local.deed_restricted_units.fillna(0) <=
#                      buildings.local.residential_units.fillna(0))

        print("Amount left after subsidy: ${:,.2f}".
              format(account.total_transactions_by_subacct(subacct)))

        new_buildings_list.append(new_buildings)

    total_len = reduce(lambda x, y: x+len(y), new_buildings_list, 0)
    if total_len == 0:
        print("No subsidized buildings")
        return

    new_buildings = pd.concat(new_buildings_list)
    print("Built {} total subsidized buildings".format(len(new_buildings)))
    print("    Total subsidy: ${:,.2f}".
          format(-1*new_buildings.max_profit.sum()))
    print("    Total subsidized units: {:.0f}".
          format(new_buildings.residential_units.sum()))

    new_buildings["subsidized"] = True
    new_buildings["policy_name"] = policy_name

    summary.add_parcel_output(new_buildings)


@orca.step()
def subsidized_residential_feasibility(
        parcels, model_settings,
        add_extra_columns_func, parcel_sales_price_sqft_func,
        parcel_is_allowed_func, parcels_geography):

    kwargs = settings['feasibility'].copy()
    kwargs["only_built"] = False
    kwargs["forms_to_test"] = ["residential"]

    config = sqftproforma.SqFtProFormaConfig()
    # use the cap rate from settings.yaml
    config.cap_rate = model_settings["cap_rate"]

    # step 1
    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          config=config,
                          **kwargs)

    feasibility = orca.get_table("feasibility").to_frame()
    # get rid of the multiindex that comes back from feasibility
    feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)
    # join to parcels_geography for filtering
    feasibility = feasibility.join(parcels_geography.to_frame(),
                                   rsuffix='_y')

    # add the multiindex back
    feasibility.columns = pd.MultiIndex.from_tuples(
            [("residential", col) for col in feasibility.columns])

#    feasibility = policy_modifications_of_profit(feasibility, parcels)
#    orca.add_table("feasibility", feasibility)

    df = orca.get_table("feasibility").to_frame()
    df = df.stack(level=0).reset_index(level=1, drop=True)


@orca.step()
def subsidized_residential_developer_vmt(
        households, buildings, add_extra_columns_func,
        parcels_geography, year, acct_settings, parcels,
        settings, summary, coffer, form_to_btype_func, feasibility):

    feasibility = feasibility.to_frame()
    feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)

    run_subsidized_developer(feasibility,
                             parcels,
                             buildings,
                             households,
                             acct_settings["vmt_settings"],
                             settings,
                             coffer["vmt_res_acct"],
                             year,
                             form_to_btype_func,
                             add_extra_columns_func,
                             summary,
                             create_deed_restricted=True,
                             policy_name="VMT")


@orca.step()
def subsidized_residential_developer_jobs_housing(
        households, buildings, add_extra_columns_func,
        parcels_geography, year, acct_settings, parcels,
        policy, summary, coffer, form_to_btype_func, settings):

    for key, acct in (policy["acct_settings"]
                      ["jobs_housing_fee_settings"].items()):

        print("Running the subsidized developer for jobs-housing acct: %s"
              % acct["name"])
        orca.eval_step("subsidized_residential_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()
        feasibility = feasibility.stack(level=0).\
            reset_index(level=1, drop=True)

        run_subsidized_developer(feasibility,
                                 parcels,
                                 buildings,
                                 households,
                                 acct,
                                 settings,
                                 coffer[acct["name"]],
                                 year,
                                 form_to_btype_func,
                                 add_extra_columns_func,
                                 summary,
                                 create_deed_restricted=acct[
                                    "subsidize_affordable"],
                                 policy_name=acct["name"])

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        orca.add_table("feasibility", pd.DataFrame())


@orca.step()
def subsidized_residential_developer_lump_sum_accts(
        households, buildings, add_extra_columns_func,
        parcels_geography, year, acct_settings, parcels,
        policy, summary, coffer, form_to_btype_func):

    for key, acct in policy["acct_settings"]["lump_sum_accounts"].items():

        print("Running the subsidized developer for acct: %s" % acct["name"])

        # need to rerun the subsidized feasibility every time and get new
        # results - this is not ideal and is a story to fix in pivotal,
        # but the only cost is in time - the results should be the same
        orca.eval_step("subsidized_residential_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()
        feasibility = feasibility.stack(level=0).\
            reset_index(level=1, drop=True)

        run_subsidized_developer(feasibility,
                                 parcels,
                                 buildings,
                                 households,
                                 acct,
                                 settings,
                                 coffer[acct["name"]],
                                 year,
                                 form_to_btype_func,
                                 add_extra_columns_func,
                                 summary,
                                 create_deed_restricted=acct[
                                    "subsidize_affordable"],
                                 policy_name=acct["name"])

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        orca.add_table("feasibility", pd.DataFrame())


@orca.step()
def subsidized_office_developer_vmt(
    parcels, settings, coffer, buildings, year, policy,
    add_extra_columns_func, summary):

    vmt_acct_settings = policy["acct_settings"]["vmt_settings"]

        print("Running subsidized office developer for acct: VMT com_for_com")

        orca.eval_step("alt_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()

        formula = vmt_acct_settings["alternate_buildings_filter"]

        print('office receiving_buildings_filter: {}'.format(formula))

        subsidized_office_developer(feasibility,
                                    coffer,
                                    formula,
                                    year,
                                    add_extra_columns_func,
                                    buildings,
                                    summary, 
                                    coffer_acct_name="vmt_com_acct")

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        # this is ok because baus.py will run alt_reasibility in the next step
        orca.add_table("feasibility", pd.DataFrame())


@orca.step()
def subsidized_office_developer_lump_sum_accts(
    parcels, settings, coffer, buildings, year, policy,
    add_extra_columns_func, summary):

    for key, acct in policy["acct_settings"]["office_lump_sum_accounts"].items():

        print("Running the subsidized office developer for acct: %s" % acct["name"])

        orca.eval_step("alt_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()

        formula = acct["receiving_buildings_filter"]
        print('office receiving_buildings_filter: {}'.format(formula))

        subsidized_office_developer(feasibility,
                                    coffer,
                                    formula,
                                    year,
                                    add_extra_columns_func,
                                    buildings,
                                    summary, 
                                    coffer_acct_name=acct["name"])

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        orca.add_table("feasibility", pd.DataFrame())
