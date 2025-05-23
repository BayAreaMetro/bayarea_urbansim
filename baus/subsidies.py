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

import logging

# Get a logger specific to this module
logger = logging.getLogger(__name__)

# this method is a custom profit to probability function where we test the
# combination of different metrics like return on cost and raw profit
def profit_to_prob_func(df):
    # the clip is because we still might build negative profit buildings
    # (when we're subsidizing them) and choice doesn't allow negative
    # probability options
    max_profit = df.max_profit.clip(1)

    factor = float(orca.get_injectable("developer_settings")[
        "profit_vs_return_on_cost_combination_factor"])

    df['return_on_cost'] = max_profit / df.total_cost

    # now we're going to make two pdfs and weight them
    ROC_p = df.return_on_cost.values / df.return_on_cost.sum()
    profit_p = max_profit / max_profit.sum()
    p = 1.0 * ROC_p + factor * profit_p

    return p / p.sum()


@orca.injectable(cache=True)
def coffer(account_strategies, run_setup):
    d = {
        "vmt_res_acct":  accounts.Account("vmt_res_acct"),
        "vmt_com_acct":  accounts.Account("vmt_com_acct")
    }

    if run_setup["run_housing_bond_strategy"]:
        for key, acct in account_strategies["acct_settings"]["lump_sum_accounts"].items():
            d[acct["name"]] = accounts.Account(acct["name"])

    if run_setup["run_office_bond_strategy"]:
        for key, acct in account_strategies["acct_settings"]["office_lump_sum_accounts"].items():
            d[acct["name"]] = accounts.Account(acct["name"])    
    
    if run_setup["run_jobs_housing_fee_strategy"]:
        for key, acct in account_strategies["acct_settings"]["jobs_housing_fee_settings"].items():
            d[acct["name"]] = accounts.Account(acct["name"])

    return d


@orca.step()
def preserve_affordable(year, base_year, preservation, residential_units, taz_geography,
                        buildings, parcels_geography, initial_summary_year):

    if not year > initial_summary_year:
        return
    
    # join several geography columns to units table so that we can apply units
    res_units = residential_units.to_frame()
    bldgs = buildings.to_frame()
    parcels_geog = parcels_geography.to_frame()
    taz_geog = taz_geography.to_frame()

    res_units = res_units.merge(bldgs[['parcel_id']], left_on='building_id', right_index=True, how='left').\
        merge(parcels_geog, left_on='parcel_id', right_index=True, how='left').\
        merge(taz_geog, left_on='zone_id', right_index=True, how='left')

    s = preservation["housing_preservation"]["settings"]

    # only preserve units that are not already deed-restricted
    res_units = res_units.loc[res_units.deed_restricted != 1]

    # initialize list of units to mark deed restricted
    dr_units = []
    
    # apply deed-restriced units by geography (county here)
    for geog, value in s.items(): 

        # apply deed-restriced units by filters within each geography 
        l = ['first', 'second', 'third', 'fourth']
        for item in l:

            if value[item+"_unit_filter"] is None or value[item+"_unit_target"] is None:
                continue
            
            filter_nm = value[item+"_unit_filter"]
            unit_target = value[item+"_unit_target"]

            # exclude units that have been preserved through this loop
            res_units = res_units[~res_units.index.isin(dr_units)]

            # subset units to the geography
            geography = preservation["housing_preservation"]["geography"]
            geog_units = res_units.loc[res_units[geography] == geog]
            # subset units to the filters within the geography
            filter_units = geog_units.query(filter_nm)

            # pull a random set of units based on the target except in cases
            # where there aren't enough units in the filtered geography or
            # they're already marked as deed restricted
            if len(filter_units) == 0:
                dr_units_set = []
                print("%s %s: target is %d but no units are available" % (geog, filter_nm, unit_target))
            elif unit_target > len(filter_units):
                 dr_units_set = filter_units.index
                 print("%s %s: target is %d but only %d units are available" % (geog, filter_nm, unit_target, len(filter_units)))
            else:
                dr_units_set = np.random.choice(filter_units.index, unit_target, replace=False)

            dr_units.extend(dr_units_set)

    # mark units as deed restriced in residential units table
    residential_units = residential_units.to_frame()
    residential_units.loc[residential_units.index.isin(dr_units), 'deed_restricted'] = 1
    orca.add_table("residential_units", residential_units)

    # mark units as deed restricted in buildings table
    buildings = buildings.to_frame(buildings.local_columns)
    new_dr_res_units = residential_units.building_id.loc[residential_units.index.isin(dr_units)].value_counts()
    buildings["preserved_units"] = (buildings["preserved_units"] + buildings.index.map(new_dr_res_units).fillna(0.0))     
    buildings["deed_restricted_units"] = (buildings["deed_restricted_units"] + buildings.index.map(new_dr_res_units).fillna(0.0))
    orca.add_table("buildings", buildings)


@orca.injectable(cache=True)
def acct_settings(account_strategies):
    return account_strategies["acct_settings"]


@orca.step()
def lump_sum_accounts(year, base_year, years_per_iter, run_setup, logger):

    if not run_setup["run_housing_bond_strategy"]:
        return
    
    account_strategies = orca.get_injectable("account_strategies")
    s = account_strategies["acct_settings"]["lump_sum_accounts"]

    coffer = orca.get_injectable("coffer")

    for key, acct in s.items():
        if not run_setup[acct["name"]]:
            continue

        amt = float(acct["total_amount"])
        amt *= years_per_iter

        # TODO: special case for 2020 base year to match 2010 base year -- add lump sum for 2015
        # I think this can be removed
        if base_year==2020 and year==2020:
            metadata = {"description": f"{acct['name']} subsidies", "year": 2015}
            coffer[acct["name"]].add_transaction(amt, subaccount=1, metadata=metadata, logger=logger)

        metadata = {"description": f"{acct['name']} subsidies", "year": year}
        # the subaccount is meaningless here (it's a regional account) but the subaccount number is referred to below
        coffer[acct["name"]].add_transaction(amt, subaccount=1, metadata=metadata, logger=logger)


@orca.step()
def office_lump_sum_accounts(run_setup, year, years_per_iter):

    if not run_setup["run_office_bond_strategy"]:
        return

    account_strategies = orca.get_injectable("account_strategies")
    s = account_strategies["acct_settings"]["office_lump_sum_accounts"]

    coffer = orca.get_injectable("coffer")

    for key, acct in s.items():

        amt = float(acct["total_amount"])
        amt *= years_per_iter

        metadata = {"description": "%s subsidies" % acct["name"], "year": year}

        # the subaccount is meaningless here (it's a regional account) -
        # but the subaccount number is referred to below
        coffer[acct["name"]].add_transaction(amt, subaccount="regional", metadata=metadata, logger=logger)

# this will compute the reduction in revenue from a project due to
# inclustionary housing - the calculation will be described in thorough
# comments alongside the code
def inclusionary_housing_revenue_reduction(feasibility, units):

    print("Computing adjustments due to inclusionary housing")

    # AMI by jurisdiction
    #
    # in practice deed restrictions are done by household size but we aren't
    # going to deed restrict them by household size so it makes sense not to
    # do that here - if we did this by household size like we do in the real
    # world we'd need to have a better representation of what household size
    # is in which unit type

    households = orca.get_table("households")
    buildings = orca.get_table("buildings")
    parcels_geography = orca.get_table("parcels_geography")

    run_setup = orca.get_injectable("run_setup")
    inclusionary = orca.get_injectable("inclusionary")
    # determine the geography type to use by reading the "type" that the first inclusionary rate is applied to, 
    # since we tend to use the same geography type for applying all of the inclusionary rates
    if run_setup["run_inclusionary_strategy"]:
        inclusionary_strategy = orca.get_injectable("inclusionary_strategy")
        geog = inclusionary_strategy["inclusionary_housing_settings"]["inclusionary_strategy"][0]["type"]
    elif "default" in inclusionary["inclusionary_housing_settings"].keys():
        geog = inclusionary["inclusionary_housing_settings"]["default"][0]["type"]
    h = orca.merge_tables("households", [households, buildings, parcels_geography], columns=["income", geog])
    AMI = h.groupby(h[geog]).income.quantile(.5)

    # per Aksel Olsen (@akselx)
    # take 90% of AMI and multiple by 33% to get the max amount a
    # household can pay per year, divide by 12 to get monthly amt,
    # subtract condo fee

    monthly_condo_fee = 250
    monthly_affordable_payment = AMI * .9 * .33 / 12 - monthly_condo_fee

    def value_can_afford(monthly_payment):
        # this is a 10 year average freddie mac interest rate
        ten_year_average_interest = .055
        return np.npv(ten_year_average_interest/12, [monthly_payment]*30*12)

    value_can_afford = {k: value_can_afford(v) for k, v in
                        monthly_affordable_payment.to_dict().items()}
    value_can_afford = pd.Series(value_can_afford)

    # account for interest and property taxes
    interest_and_prop_taxes = .013
    value_can_afford /= 1+interest_and_prop_taxes

    # there's a lot more nuance to inclusionary percentages than this -
    # e.g. specific neighborhoods get specific amounts -
    # http://sf-moh.org/modules/showdocument.aspx?documentid=7253

    pct_inclusionary = orca.get_injectable("inclusionary_housing_settings")

    # calculate revenue reduction by strategy geographies
    geography = parcels_geography[geog].loc[feasibility.index]
    pct_affordable = geography.map(pct_inclusionary).fillna(0)
    value_can_afford = geography.map(value_can_afford)

    num_affordable_units = (units * pct_affordable).fillna(0).astype("int")

    ave_price_per_unit = feasibility[('residential', 'building_revenue')] / units

    revenue_diff_per_unit = (ave_price_per_unit - value_can_afford).fillna(0)
    print("Revenue difference per unit (not zero values)")
    print(revenue_diff_per_unit[revenue_diff_per_unit > 0].describe())

    revenue_reduction = revenue_diff_per_unit * num_affordable_units

    s = num_affordable_units.groupby(parcels_geography[geog]).sum()
    print("Feasibile affordable units by geography")
    print(s[s > 0].sort_values())

    return revenue_reduction, num_affordable_units


# this adds fees to the max_profit column of the feasibility dataframe
# fees are usually spatially specified and are per unit so that calculation is done here as well
def policy_modifications_of_profit(feasibility, parcels):

    print("Making policy modifications to profitability")

    units = feasibility[('residential', 'residential_sqft')] / parcels.ave_sqft_per_unit

    # this first section adds parcel unit-based fees
    run_setup = orca.get_injectable("run_setup")

    if run_setup["run_vmt_fee_strategy"]:

        fees = (units * parcels.fees_per_unit).fillna(0)
        print("Sum of residential fees: ", fees.sum())

        feasibility[("residential", "fees")] = fees
        feasibility[("residential", "max_profit")] -= fees

        #  now non residential fees per sqft
        for use in ["retail", "office"]:

            if (use, 'non_residential_sqft') not in feasibility.columns:
                continue

            sqft = feasibility[(use, 'non_residential_sqft')]
            fees = (sqft * parcels.fees_per_sqft).fillna(0)
            print("Sum of non-residential fees (%s): %.0f" % (use, fees.sum()))

            feasibility[(use, "fees")] = fees
            feasibility[(use, "max_profit")] -= fees

    # this section adds inclusionary housing reduction in revenue
    revenue_reduction, num_affordable_units = inclusionary_housing_revenue_reduction(feasibility, units)

    assert np.all(num_affordable_units <= units.fillna(0))

    print("Describe of inclusionary revenue reduction:\n", revenue_reduction[revenue_reduction > 0].describe())

    print("Describe of number of affordable units:\n", num_affordable_units[num_affordable_units > 0].describe())

    feasibility[("residential", "policy_based_revenue_reduction")] = revenue_reduction
    feasibility[("residential", "max_profit")] -= revenue_reduction
    feasibility[("residential", "deed_restricted_units")] = num_affordable_units
    feasibility[("residential", "inclusionary_units")] = num_affordable_units

    profit_adjustment_strategies = orca.get_injectable("profit_adjustment_strategies")

    if run_setup["run_sb_743_strategy"]:

        sb743_settings = profit_adjustment_strategies["acct_settings"]["sb743_settings"]

        pct_modifications = feasibility[("residential", "vmt_res_cat")].map(sb743_settings["sb743_pcts"]) + 1
        
        print("Modifying profit for SB743:\n", pct_modifications.describe())
        feasibility[("residential", "max_profit")] *= pct_modifications

    if run_setup["run_land_value_tax_strategy"]:

        s = profit_adjustment_strategies["acct_settings"]["land_value_tax_settings"]

        bins = s["bins"]
        pcts = bins["pcts"]
        # need to boud the breaks with a reasonable low and high goalpost
        breaks = [-1]+bins["breaks"]+[2]

        pzc = orca.get_table("parcels_zoning_calculations")
        s = pzc.zoned_build_ratio
        # map the breakpoints defined in yaml to the pcts defined there
        pct_modifications = pd.cut(s, breaks, labels=pcts).astype('float') + 1
        # if some parcels got skipped, fill them in with no modification
        pct_modifications = pct_modifications.reindex(pzc.index).fillna(1.0)

        print("Modifying profit for Land Value Tax:\n", pct_modifications.describe())

        feasibility[("residential", "max_profit")] *= pct_modifications


    if "profitability_adjustment_policies" in profit_adjustment_strategies["acct_settings"]:

        tier_cols = []
        for key, policy in profit_adjustment_strategies["acct_settings"]["profitability_adjustment_policies"].items():

            if run_setup[policy["name"]]:

                parcels_geography = orca.get_table("parcels_geography")

                formula_segment = policy["profitability_adjustment_formula"]
                formula_value = policy["profitability_adjustment_value"]

                # Evaluate tier segment
                pcl_formula_segment = parcels_geography.local.eval(formula_segment).astype(int)
                print(f'profit_adjust_tier distribution for {key}')
                print(pcl_formula_segment.value_counts())

                # Multiply (0,1) series with adjustment value
                pct_modifications = pcl_formula_segment.mul(formula_value)

                # Convert to practical factors by adding 1
                #pct_modifications += 1

                # Assign classification column back to parcels_geography frame
                this_tier = policy['shortname']
                tier_cols.append(this_tier)
                orca.add_column('parcels', this_tier, pcl_formula_segment)

                print("Modifying profit for %s:\n" % policy["name"], (1+pct_modifications).describe())
                print(f"Formula for {this_tier}: \n{formula_segment}: {formula_value:0.2%}")

                # Adjust max_profit while accounting for both positive and negative values. 
                # Fixing issue of magnifying negative profits rather than reducing them due to multiplication
                feasibility[("residential", "max_profit")] = (
                    feasibility[("residential", "max_profit")] + (
                        feasibility[("residential", "max_profit")].abs()
                        * (pct_modifications)
                    )
                )

        if len(tier_cols)>0:
            print(f'tier_cols: {tier_cols}')
            hsg_tier_group = parcels.to_frame(columns=tier_cols).groupby(tier_cols).ngroup()
            hsg_tier_group.to_csv('subsidy_hsg_tier_group.csv')
            orca.add_column('parcels', 'hsg_tier_grp', hsg_tier_group)

    print("There are %d affordable units if all feasible projects are built" % feasibility[("residential", "deed_restricted_units")].sum())

    return feasibility




@orca.step()
def calculate_vmt_fees(run_setup, account_strategies, year, buildings, coffer, summary, years_per_iter):

    vmt_settings = account_strategies["acct_settings"]["vmt_settings"]

    # this is the frame that knows which devs are subsidized
    df = summary.parcel_output

    # grabs projects in the simulation period that are not subsidized
    df = df.query("%d <= year_built < %d and subsidized != True" % (year, year + years_per_iter))

    if not len(df):
        return

    print("%d projects pass the vmt filter" % len(df))

    total_fees = 0

    if run_setup["run_vmt_fee_res_for_res_strategy"]:

        # maps the vmt fee amounts designated in the policy settings to
        # the projects based on their categorized vmt levels
        df["res_for_res_fees"] = df.vmt_res_cat.map(vmt_settings["res_for_res_fee_amounts"])
        total_fees += (df.res_for_res_fees * df.residential_units).sum()
        print("Applying vmt fees to %d units" % df.residential_units.sum())

    if run_setup["run_vmt_fee_com_for_res_strategy"]:

        df["com_for_res_fees"] = df.vmt_nonres_cat.map( vmt_settings["com_for_res_fee_amounts"])
        total_fees += (df.com_for_res_fees * df.non_residential_sqft).sum()
        print("Applying vmt fees to %d commerical sqft" % df.non_residential_sqft.sum())

    print("Adding total vmt fees for res amount of $%.2f" % total_fees)

    metadata = {"description": "VMT development fees", "year": year}

    # the subaccount is meaningless here (it's a regional account) - but the subaccount number is referred to below
    # adds the total fees collected to the coffer for residential dev
    coffer["vmt_res_acct"].add_transaction(total_fees, subaccount=1, metadata=metadata, logger=logger)

    total_fees = 0
    if run_setup["run_vmt_fee_com_for_com_strategy"]:

        # assign fees by county
        # assign county to parcels
        county_lookup = orca.get_table("parcels_subzone").to_frame()
        county_lookup = county_lookup[["county"]].rename(columns={'county': 'county3'})
        county_lookup.reset_index(inplace=True)
        county_lookup = county_lookup.rename(columns={'PARCEL_ID': 'PARCELID'})
        df = df.merge(county_lookup, left_on='parcel_id', right_on='PARCELID', how='left')

        # assign fee to parcels based on county
        counties3 = ['ala', 'cnc', 'mar', 'nap', 'scl', 'sfr', 'smt', 'sol', 'son']
        counties = ['alameda', 'contra_costa', 'marin', 'napa', 'santa_clara', 'san_francisco', 'san_mateo', 'solano', 'sonoma']
        for county3, county in zip(counties3, counties):
            df.loc[df["county3"] == county3, "com_for_com_fees"] = df.vmt_nonres_cat.map(vmt_settings["com_for_com_fee_amounts"][county])

        total_fees += (df.com_for_com_fees * df.non_residential_sqft).sum()
        print("Applying vmt fees to %d commerical sqft" % df.non_residential_sqft.sum())

    print("Adding total vmt fees for com amount of $%.2f" % total_fees)

    coffer["vmt_com_acct"].add_transaction(total_fees, subaccount="regional", metadata=metadata, logger=logger)


@orca.step()
def calculate_jobs_housing_fees(account_strategies, year, coffer, summary, years_per_iter):

    jobs_housing_settings = account_strategies["acct_settings"]["jobs_housing_fee_settings"]

    # this is the frame that knows which devs are subsidized
    df = summary.parcel_output

    df = df.query("%d <= year_built < %d and subsidized != True" % (year, year + years_per_iter))

    if not len(df):
        return

    print("%d projects pass the jobs_housing filter" % len(df))

    for key, acct in jobs_housing_settings.items():
 
        # assign jurisdiction to parcels
        juris_lookup = orca.get_table("parcels_geography").to_frame()
        juris_lookup = juris_lookup[['PARCEL_ID', 'juris_name']].rename(columns={'PARCEL_ID': 'PARCELID', 'juris_name': 'jurisname'})

        county_lookup = orca.get_table("parcels_subzone").to_frame().reset_index()
        county_lookup = county_lookup[['PARCEL_ID', 'county']].rename(columns={'PARCEL_ID': 'PARCELID', 'county': 'county3'})

        df = df.merge(juris_lookup, left_on='parcel_id', right_on='PARCELID', how='left').merge(county_lookup, on='PARCELID', how='left')

        # calculate jobs-housing fees for each county's acct
        df_sub = df.loc[df.county3 == acct["county_name"]]

        print("Applying jobs-housing fees to %d commerical sqft" % df_sub.non_residential_sqft.sum())

        total_fees = 0
        df_sub["com_for_res_jobs_housing_fees"] = df_sub.jurisname.map(acct["jobs_housing_fee_com_for_res_amounts"])
        total_fees += (df_sub.com_for_res_jobs_housing_fees * df_sub.non_residential_sqft).sum()

        print("Adding total jobs-housing fees for res amount of $%.2f" % total_fees)

        metadata = {"description": "%s subsidies from jobs-housing development fees" % acct["name"], "year": year}

        # add to the subaccount in coffer
        coffer[acct["name"]].add_transaction(total_fees, subaccount=acct["name"], metadata=metadata, logger=logger)


#@orca.step()
def subsidized_office_developer(feasibility, coffer, formula, year, add_extra_columns_func, buildings, summary, coffer_acct_name):

    # get the total subsidy for subsidizing office
    total_subsidy = coffer[coffer_acct_name].total_transactions_by_subacct("regional")

    # get the office feasibility frame and sort by profit per sqft
    
    feasibility = feasibility.loc[:, "office"]

    feasibility = feasibility.dropna(subset=["max_profit"])

    # filter to receiving zone
    feasibility = feasibility.query(formula)

    feasibility["non_residential_sqft"] = feasibility.non_residential_sqft.astype("int")

    feasibility["max_profit_per_sqft"] = feasibility.max_profit / feasibility.non_residential_sqft

    # sorting by our choice metric as we're going to pick our devs
    # in order off the top
    feasibility = feasibility.sort_values(['max_profit_per_sqft'])

    # make parcel_id available
    feasibility = feasibility.reset_index()

    print("%.0f subsidy with %d developments to choose from" % (total_subsidy, len(feasibility)))

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
            amt = (NORMAL_PROFIT_PER_SQFT - d.max_profit_per_sqft) * d.non_residential_sqft

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

        coffer[coffer_acct_name].add_transaction(-1*amt, subaccount="regional", metadata=metadata, logger=logger)

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


def run_subsidized_developer(feasibility, parcels, buildings, households, acct_settings, developer_settings, account, year, form_to_btype_func, 
                             add_extra_columns_func, summary, create_deed_restricted=False, policy_name="Unnamed"):
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
        A dictionary of settings to parameterize the model.  Needs these keys: sending_buildings_subaccount_def - maps buildings to subaccounts
        receiving_buildings_filter - filter for eligible buildings
    developer_settings : Dict
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
        Bool for whether to create deed restricted units with the subsidies or not.  The logic at the time of this writing is to keep track of
        partial units so that when partial units sum to greater than a unit, that unit will be deed restricted.

    Returns
    -------
    Nothing

    Subsidized residential developer is designed to run before the normal residential developer - it will prioritize the developments we're
    subsidizing (although this is not strictly required - running this model after the market rate developer will just create a temporarily larger
    supply of units, which will probably create less market rate development in the next simulated year). The steps for subsidizing are 
    essentially these:

    1 run feasibility with only_built set to false so that the feasibility of unprofitable units are recorded
    2 temporarily filter to ONLY unprofitable units to check for possible subsidized units (normal developer takes care of market-rate units)
    3 compute the number of units in these developments
    4 divide cost by number of units in order to get the subsidy per unit
    5 filter developments to parcels in "receiving zone" similar to the way we identified "sending zones"
    6 iterate through subaccounts one at a time as subsidy will be limited to available funds in the subaccount (usually by jurisdiction)
    7 sort ascending by subsidy per unit so that we minimize subsidy (but total subsidy is equivalent to total building cost)
    8 cumsum the total subsidy in the buildings and locate the development
        where the subsidy is less than or equal to the amount in the account - filter to only those buildings (these will likely be built)
    9 pass the results as "feasible" to run_developer - this is sort of a boundary case of developer but should run OK
    10 for those developments that get built, make sure to subtract from account and keep a record (on the off chance that demand is less than
        the subsidized units, run through the standard code path, although it's very unlikely that there would be more subsidized housing than 
        demand)
    """
    feas_cols = sorted(feasibility.columns.to_list())
    logger.debug("run_subsidized_developer(): feasibility len={:,} dataframe=\n{}".format(
        len(feasibility), feasibility[feas_cols]
    ))
    # step 2
    feasibility = feasibility.replace([np.inf, -np.inf], np.nan)
    feasibility = feasibility[feasibility.max_profit < 0]

    # step 3
    feasibility['ave_sqft_per_unit'] = parcels.ave_sqft_per_unit
    feasibility['residential_units'] = np.floor(feasibility.residential_sqft / feasibility.ave_sqft_per_unit)

    # step 3B
    # can only add units - don't subtract units - this is an approximation
    # of the calculation that will be used to do this in the developer model
    feasibility = feasibility[feasibility.residential_units > feasibility.total_residential_units]

    # step 3C
    # towards the end, because we're about to sort by subsidy per unit, some
    # large projects never get built, because it could be a 100 unit project
    # times a 500k subsidy per unit.  thus we're going to try filtering by
    # the maximum subsidy for a single development here
    feasibility = feasibility[feasibility.max_profit > -50*1000000]

    # step 4
    feasibility['subsidy_per_unit'] = -1 * feasibility['max_profit'] / feasibility['residential_units']
    # assumption that even if the developer says this property is almost
    # profitable, even the administration costs are likely to cost at least
    # 10k / unit
    feasibility['subsidy_per_unit'] = feasibility.subsidy_per_unit.clip(10000)
       
    # step 5
    if "receiving_buildings_filter" in acct_settings:
        feasibility = feasibility.query(acct_settings["receiving_buildings_filter"])
    else:
        # otherwise all buildings are valid
        pass

    new_buildings_list = []
    sending_bldgs = acct_settings["sending_buildings_subaccount_def"]
    feasibility["regional"] = 1
    feasibility["subaccount"] = feasibility.eval(sending_bldgs)
    # step 6
    for subacct, amount in account.iter_subaccounts():
        logger.debug("Subaccount:{}; amount:${:,.2f}".format(subacct, amount))

        df = feasibility[feasibility.subaccount == subacct]
        logger.debug("Number of feasible projects in receiving zone: {:,}".format(len(df)))

        if len(df) == 0:
            continue

        # step 7
        df = df.sort_values(['subsidy_per_unit'], ascending=True)
        # df.to_csv('subsidized_units_%d_%s_%s.csv' % (orca.get_injectable("year"), account.name, subacct))

        # step 8
        num_bldgs = int((-1*df.max_profit).cumsum().searchsorted(amount))

        if num_bldgs == 0:
            continue

        # technically we only build these buildings if there's demand
        # print "Building {:d} subsidized buildings".format(num_bldgs)
        df = df.iloc[:int(num_bldgs)]

        df.columns = pd.MultiIndex.from_tuples(
            [("residential", col) for col in df.columns])

        kwargs = developer_settings['residential_developer']
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

                revenue_per_unit = new_building.building_revenue / new_building.residential_units
                total_subsidy = abs(new_building.max_profit)
                subsidized_units = total_subsidy / revenue_per_unit + partial_subsidized_units
                # right now there are inclusionary requirements
                already_subsidized_units = new_building.deed_restricted_units

                # get remainder
                partial_subsidized_units = subsidized_units % 1
                # round off for now
                subsidized_units = int(subsidized_units) + already_subsidized_units
                # cap at number of residential units
                subsidized_units = min(subsidized_units, new_building.residential_units)

                buildings.local.loc[index, "deed_restricted_units"] = int(round(subsidized_units))

                buildings.local.loc[index, "subsidized_units"] = buildings.local.loc[index, "deed_restricted_units"] - \
                    buildings.local.loc[index, "inclusionary_units"]

                # also correct the debug output
                new_buildings.loc[index, "deed_restricted_units"] = int(round(subsidized_units))
                new_buildings.loc[index, "subsidized_units"] = new_buildings.loc[index, "deed_restricted_units"] - \
                    new_buildings.loc[index, "inclusionary_units"]

            metadata['deed_restricted_units'] = new_buildings.loc[index, 'deed_restricted_units']
            metadata['subsidized_units'] = new_buildings.loc[index, 'subsidized_units']
            account.add_transaction(amt, subaccount=subacct, metadata=metadata,  logger=logger)

        # turn off this assertion for the Draft Blueprint affordable housing policy since the number of deed restricted units
        # vs units from development projects looks reasonable
#        assert np.all(buildings.local.deed_restricted_units.fillna(0) <=
#                      buildings.local.residential_units.fillna(0))

        logger.debug("Amount left after subsidy: ${:,.2f}".format(account.total_transactions_by_subacct(subacct)))

        new_buildings_list.append(new_buildings)

    total_len = reduce(lambda x, y: x+len(y), new_buildings_list, 0)
    if total_len == 0:
        logger.debug("No subsidized buildings")
        return

    new_buildings = pd.concat(new_buildings_list)
    logger.debug("Built {:,} total subsidized buildings".format(len(new_buildings)))
    logger.debug("    Total subsidy: ${:,.2f}".format(-1*new_buildings.max_profit.sum()))
    logger.debug("    Total subsidized units: {:.0f}".format(new_buildings.residential_units.sum()))

    new_buildings["subsidized"] = True
    new_buildings["policy_name"] = policy_name


@orca.step()
def subsidized_residential_feasibility(parcels, developer_settings, parcel_sales_price_sqft_func, parcel_is_allowed_func, 
                                       parcels_geography):

    kwargs = developer_settings['feasibility'].copy()
    kwargs["only_built"] = False
    kwargs["forms_to_test"] = ["residential"]

    config = sqftproforma.SqFtProFormaConfig()
    # use the cap rate from settings.yaml
    config.cap_rate = developer_settings["cap_rate"]

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
    feasibility.columns = pd.MultiIndex.from_tuples([("residential", col) for col in feasibility.columns])

    feasibility = policy_modifications_of_profit(feasibility, parcels)

    orca.add_table("feasibility", feasibility)

    df = orca.get_table("feasibility").to_frame()
    df = df.stack(level=0).reset_index(level=1, drop=True)
    # this uses a surprising amount of disk space, don't write out for now
    # df.to_csv(os.path.join(orca.get_injectable("outputs_dir), "feasibility_{}.csv".format(orca.get_injectable("year")))


@orca.step()
def subsidized_residential_developer_vmt(households, buildings, add_extra_columns_func, parcels_geography, year, acct_settings, parcels,
                                         developer_settings, summary, coffer, form_to_btype_func, feasibility):

    feasibility = feasibility.to_frame()
    feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)

    run_subsidized_developer(feasibility,
                             parcels,
                             buildings,
                             households,
                             acct_settings["vmt_settings"],
                             developer_settings,
                             coffer["vmt_res_acct"],
                             year,
                             form_to_btype_func,
                             add_extra_columns_func,
                             summary,
                             create_deed_restricted=True,
                             policy_name="VMT")


@orca.step()
def subsidized_residential_developer_jobs_housing(households, buildings, add_extra_columns_func, parcels_geography, year, parcels,
                                                  summary, coffer, form_to_btype_func, developer_settings, account_strategies):

    for key, acct in (account_strategies["acct_settings"]["jobs_housing_fee_settings"].items()):

        print("Running the subsidized developer for jobs-housing acct: %s" % acct["name"])

        orca.eval_step("subsidized_residential_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()

        feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)

        run_subsidized_developer(feasibility,
                                 parcels,
                                 buildings,
                                 households,
                                 acct,
                                 developer_settings,
                                 coffer[acct["name"]],
                                 year,
                                 form_to_btype_func,
                                 add_extra_columns_func,
                                 summary,
                                 create_deed_restricted=acct["subsidize_affordable"],
                                 policy_name=acct["name"])

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        orca.add_table("feasibility", pd.DataFrame())


@orca.step()
def subsidized_residential_developer_lump_sum_accts(run_setup, households, buildings, add_extra_columns_func, parcels_geography, year, 
                                                    parcels, summary, form_to_btype_func, developer_settings):
    

    if not run_setup["run_housing_bond_strategy"]:
        return
    
    account_strategies = orca.get_injectable("account_strategies")
    coffer = orca.get_injectable("coffer")

    for key, acct in account_strategies["acct_settings"]["lump_sum_accounts"].items():

        if not run_setup[acct["name"]]:
            continue

        print("Running the subsidized developer for acct: %s" % acct["name"])

        # need to rerun the subsidized feasibility every time and get new
        # results - this is not ideal and is a story to fix in pivotal,
        # but the only cost is in time - the results should be the same
        orca.eval_step("subsidized_residential_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()
        feasibility = feasibility.stack(level=0).reset_index(level=1, drop=True)

        run_subsidized_developer(feasibility,
                                 parcels,
                                 buildings,
                                 households,
                                 acct,
                                 developer_settings,
                                 coffer[acct["name"]],
                                 year,
                                 form_to_btype_func,
                                 add_extra_columns_func,
                                 summary,
                                 create_deed_restricted=acct["subsidize_affordable"],
                                 policy_name=acct["name"])

        buildings = orca.get_table("buildings")

        # set to an empty dataframe to save memory
        orca.add_table("feasibility", pd.DataFrame())


@orca.step()
def subsidized_office_developer_vmt(run_setup, parcels, coffer, buildings, year, account_strategies, add_extra_columns_func, summary):

    vmt_acct_settings = account_strategies["acct_settings"]["vmt_settings"]

    if run_setup["vmt_fee_com_for_com"]:

        print("Running subsidized office developer for acct: VMT com_for_com")

        orca.eval_step("alt_feasibility")
        feasibility = orca.get_table("feasibility").to_frame()

        formula = vmt_acct_settings["receiving_buildings_filter"]

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
def subsidized_office_developer_lump_sum_accts(run_setup, buildings, year, add_extra_columns_func, summary):

    if not run_setup["run_office_bond_strategy"]:
        return
    
    account_strategies = orca.get_injectable("account_strategies")
    coffer = orca.get_injectable("coffer")
    
    for key, acct in account_strategies["acct_settings"]["office_lump_sum_accounts"].items():

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
