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

# UTILS?

# this method is a custom profit to probability function where we test the
# combination of different metrics like return on cost and raw profit
def profit_to_prob_func(df):
    # the clip is because we still might build negative profit buildings
    # (when we're subsidizing them) and choice doesn't allow negative
    # probability options
    max_profit = df.max_profit.clip(1)

    factor = float(orca.get_injectable("settings")[
        "profit_vs_return_on_cost_combination_factor"])

    df['return_on_cost'] = max_profit / df.total_cost

    # now we're going to make two pdfs and weight them
    ROC_p = df.return_on_cost.values / df.return_on_cost.sum()
    profit_p = max_profit / max_profit.sum()
    p = 1.0 * ROC_p + factor * profit_p

    return p / p.sum()


@orca.injectable(cache=True)
def coffer():
    d = {
        "vmt_res_acct":  accounts.Account("vmt_res_acct"),
        "vmt_com_acct":  accounts.Account("vmt_com_acct")
    }

    for key, acct in \
            policy["acct_settings"]["lump_sum_accounts"].items():
        d[acct["name"]] = accounts.Account(acct["name"])

    for key, acct in \
            policy["acct_settings"]["office_lump_sum_accounts"].items():
        d[acct["name"]] = accounts.Account(acct["name"])    

    for key, acct in \
            policy["acct_settings"]["jobs_housing_fee_settings"].items():
        d[acct["name"]] = accounts.Account(acct["name"])

    return d


@orca.step()
def preserve_affordable(preservation_policy, year, base_year, residential_units,
                        taz_geography, buildings, parcels_geography):


    # join several geography columns to units table so that we can apply units
    res_units = residential_units.to_frame()
    bldgs = buildings.to_frame()
    parcels_geog = parcels_geography.to_frame()
    taz_geog = taz_geography.to_frame()

    res_units = res_units.merge(bldgs[['parcel_id']], left_on='building_id', right_index=True, how='left').\
                          merge(parcels_geog[['gg_id', 'sesit_id', 'tra_id', 'juris']], left_on='parcel_id', 
                                right_index=True, how='left').\
                          merge(taz_geog, left_on='zone_id', right_index=True, how='left')

    # only preserve units that are not already deed-restricted
    res_units = res_units.loc[res_units.deed_restricted != 1]
    # initialize list of units to mark deed restricted
    dr_units = []
    
    # apply deed-restriced units by filter within each geography 
    for i in preservation_policy: 
        
        if preservation_policy[i].unit_target is None
        continue
         
        unit_target = preservation_policy[i].unit_target

        # exclude units that have been preserved through this loop
        res_units = res_units[~res_units.index.isin(dr_units)]

        # subset units to the geography and filter area
        geog_units = res_units.loc[res_units[geography] == preservation_policy[i].geography]
        filter_units = geog_units.query(preservation_policy[i].unit_filter)

        # pull a random set of units based on the target except in cases
        # where there aren't enough units in the filtered geography or
        # they're already marked as deed restricted
        if len(filter_units) == 0:
            dr_units_set = []
        elif unit_target > len(filter_units):
            dr_units_set = filter_units.index
        else:
            dr_units_set = np.random.choice(filter_units.index, 
                                            unit_target, replace=False)

        dr_units.extend(dr_units_set)

    # mark units as deed restriced in residential units table
    residential_units = residential_units.to_frame()
    residential_units.loc[residential_units.index.isin(dr_units), 
                          'deed_restricted'] = 1
    orca.add_table("residential_units", residential_units)

    # mark units as deed restricted in buildings table
    buildings = buildings.to_frame(buildings.local_columns)
    new_dr_res_units = residential_units.building_id.loc[residential_units.\
        index.isin(dr_units)].value_counts()
    buildings["preserved_units"] = (buildings["preserved_units"] + 
        buildings.index.map(new_dr_res_units).fillna(0.0))     
    buildings["deed_restricted_units"] = (buildings["deed_restricted_units"] + 
        buildings.index.map(new_dr_res_units).fillna(0.0))
    orca.add_table("buildings", buildings)


@orca.step()
def residential_lump_sum_accounts(year, buildings, coffer,
                      summary, years_per_iter):

    s = policy["acct_settings"]["lump_sum_accounts"]

    for key, acct in s.items():

            amt = float(acct["total_amount_fb"])

        amt *= years_per_iter

        metadata = {
            "description": "%s subsidies" % acct["name"],
            "year": year
        }
        # the subaccount is meaningless here (it's a regional account) -
        # but the subaccount number is referred to below
        coffer[acct["name"]].add_transaction(amt, subaccount=1,
                                             metadata=metadata)

@orca.step()
def office_lump_sum_accounts(policy, year, buildings, coffer,
                             summary, years_per_iter):

    s = policy["acct_settings"]["office_lump_sum_accounts"]

    for key, acct in s.items():


        amt = float(acct["total_amount"])

        amt *= years_per_iter

        metadata = {
            "description": "%s subsidies" % acct["name"],
            "year": year
        }
        # the subaccount is meaningless here (it's a regional account) -
        # but the subaccount number is referred to below
        coffer[acct["name"]].add_transaction(amt, subaccount="regional",
                                             metadata=metadata)


@orca.step()
def calculate_vmt_fees(vmt_fees, year, buildings, vmt_fee_categories, coffer,
                       summary, years_per_iter):

    vmt_settings = vmt_fees

    # this is the frame that knows which devs are subsidized
    df = summary.parcel_output

    # grabs projects in the simulation period that are not subsidized
    df = df.query("%d <= year_built < %d and subsidized != True" %
                  (year, year + years_per_iter))

    if not len(df):
        return

    print("%d projects pass the vmt filter" % len(df))

    total_fees = 0

    # maps the vmt fee amounts designated in the policy settings to
    # the projects based on their categorized vmt levels
    df["res_for_res_fees"] = df.vmt_res_cat.map(
        vmt_settings["res_for_res_fee_amounts"])
    total_fees += (df.res_for_res_fees * df.residential_units).sum()
    print("Applying vmt fees to %d units" % df.residential_units.sum())

    df["com_for_res_fees"] = df.vmt_nonres_cat.map(
                vmt_settings["alternate_com_for_res_fee_amounts"])
    total_fees += (df.com_for_res_fees * df.non_residential_sqft).sum()
    print("Applying vmt fees to %d commerical sqft" %
              df.non_residential_sqft.sum())

    print("Adding total vmt fees for res amount of $%.2f" % total_fees)

    metadata = {
        "description": "VMT development fees",
        "year": year
    }
    # the subaccount is meaningless here (it's a regional account) -
    # but the subaccount number is referred to below
    # adds the total fees collected to the coffer for residential dev
    coffer["vmt_res_acct"].add_transaction(total_fees, subaccount=1,
                                           metadata=metadata)

    total_fees = 0

    # assign county to parcels
    county_lookup = orca.get_table("parcels_subzone").to_frame()
    county_lookup = county_lookup[["county"]].\
        rename(columns={'county': 'county3'})
    county_lookup.reset_index(inplace=True)
    county_lookup = county_lookup.\
        rename(columns={'PARCEL_ID': 'PARCELID'})

    df = df.merge(county_lookup,
                  left_on='parcel_id',
                  right_on='PARCELID',
                  how='left')
    # assign fee to parcels based on county
    counties3 = ['ala', 'cnc', 'mar', 'nap', 'scl', 'sfr', 'smt',
                 'sol', 'son']
    counties = ['alameda', 'contra_costa', 'marin', 'napa',
                'santa_clara', 'san_francisco', 'san_mateo',
                'solano', 'sonoma']
    for county3, county in zip(counties3, counties):
        df.loc[df["county3"] == county3, "com_for_com_fees"] = \
            df.vmt_nonres_cat.\
            map(vmt_settings["db_com_for_com_fee_amounts"][county])

    df["com_for_com_fees"] = df.vmt_nonres_cat.map(
        vmt_settings["alternate_com_for_com_fee_amounts"])

    total_fees += (df.com_for_com_fees * df.non_residential_sqft).sum()
    print("Applying vmt fees to %d commerical sqft" %
      df.non_residential_sqft.sum())

    print("Adding total vmt fees for com amount of $%.2f" % total_fees)

    coffer["vmt_com_acct"].add_transaction(total_fees, subaccount="regional",
                                           metadata=metadata)


@orca.step()
def calculate_jobs_housing_fees(jobs_housing_fees, year, buildings,
                                coffer, summary, years_per_iter):

    # this is the frame that knows which devs are subsidized
    df = summary.parcel_output

    df = df.query("%d <= year_built < %d and subsidized != True" %
                  (year, year + years_per_iter))

    if not len(df):
        return

    # apply com_for_res fees to the dataframe

    for i in jobs_housings_fees:
        geography = jobs_housing_fees[i].receiving_buildings_geography
        df = df.loc[df.geography == jobs_housing_fees[i].receiving_buildings_filter]
        print("Applying jobs-housing fees to %d commerical sqft" % df.non_residential_sqft.sum())

        total_fees = 0
        df["com_for_res_jobs_housing_fees"] = jobs_houing_fees[i].fees_per_sqft
        total_fees += (df.com_for_res_jobs_housing_fees * df.non_residential_sqft).sum()
        print("Adding total jobs-housing fees for res amount of $%.2f" % total_fees)

        metadata = {"description": "%s subsidies from jobs-housing development fees" % jobs_housing_fees[i].acct,"year": year}

        # add to the subaccount in coffer
        coffer[acct["name"]].add_transaction(total_fees, jobs_housing_fees[i].name, metadata=metadata)