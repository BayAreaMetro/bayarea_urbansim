from __future__ import print_function
import orca
import pandas as pd
import numpy as np
from urbansim import accounts
from urbansim_defaults import utils
from urbansim.utils import misc
from baus.utils import add_buildings



@orca.injectable(cache=True)
def coffer():

	d = {"vmt_res_acct":  accounts.Account("vmt_res_acct"), 
         "vmt_com_acct":  accounts.Account("vmt_com_acct")}

	for i in housing_bonds.to_frame()
		d[housing_bonds[i].name] = accounts.Account(housing_bonds[i].name)

    return d


@orca.step()
def lump_sum_accounts_housing_bonds(housing_bonds, year, buildings, coffer, summary, years_per_iter):

    for i in housing_bonds.to_frame():
        amt = float(housing_bonds[i].total_amount)
        amt *= years_per_iter
        metadata = {"description": "%s subsidies" % housing_bonds[i].name, "year": year}
        # add the amount to the coffer
        coffer[housing_bonds[i].name].add_transaction(amt, subaccount=1, metadata=metadata)

@orca.step()
def lump_sum_accounts_office_bonds(office_bonds, year, buildings, coffer, summary, years_per_iter):

    for i in office_bonds.to_frame():
        amt = float(office_bonds[i].total_amount)
        amt *= years_per_iter
        metadata = {"description": "%s subsidies" % office_bonds[i].name, "year": year}
        # add the amount to the coffer
        coffer[office_bonds[i].name].add_transaction(amt, subaccount=1, metadata=metadata)


@orca.step()
def calculate_vmt_fees(vmt_fees, year, buildings, vmt_fee_categories, coffer, summary, years_per_iter):

    # select projects in the simulation period that are not subsidized
    df = summary.parcel_output
    df = df.query("%d <= year_built < %d and subsidized != True" % (year, year + years_per_iter))

    if not len(df):
        return

    print("%d projects pass the vmt filter" % len(df))

    # fees for residential subisdy- map the vmt fee amounts designated in the vmt fee settings 
    # to residential/commercial projects based on their categorized vmt levels
    total_fees = 0

    df["res_for_res_fees"] = df.vmt_res_cat.map(vmt_settings.res_for_res_fee_amounts)
    total_fees += (df.res_for_res_fees * df.residential_units).sum()
    print("Applying vmt fees to %d units" % df.residential_units.sum())

    df["com_for_res_fees"] = df.vmt_nonres_cat.map(vmt_settings.com_for_res_fee_amounts)
    total_fees += (df.com_for_res_fees * df.non_residential_sqft).sum()
    print("Applying vmt fees to %d commerical sqft" % df.non_residential_sqft.sum())

    print("Adding total vmt fees for res amount of $%.2f" % total_fees)
    metadata = {"description": "VMT development fees", "year": year}
    # add the total fees collected to the coffer for residential development
    coffer["vmt_res_acct"].add_transaction(total_fees, subaccount=1, metadata=metadata)

    # fees for commercial subsidy - map the vmt fee amounts designated in the vmt fee settings 
    # to residential/commercial projects based on their categorized vmt levels
    total_fees = 0

    df["com_for_com_fees"] = df.vmt_nonres_cat.map(vmt_settings["com_for_com_fee_amounts"])
    total_fees += (df.com_for_com_fees * df.non_residential_sqft).sum()
    print("Applying vmt fees to %d commerical sqft" %df.non_residential_sqft.sum())
 
    print("Adding total vmt fees for com amount of $%.2f" % total_fees)
    metadata = {"description": "VMT development fees", "year": year}
    # add the total fees collected to the coffer for commercial development
    coffer["vmt_com_acct"].add_transaction(total_fees, subaccount=1 metadata=metadata)


@orca.step()
def calculate_jobs_housing_fees(jobs_housing_fees, year, buildings, coffer, summary, years_per_iter):

    # select projects in the simulation period that are not subsidized
    df = summary.parcel_output
    df = df.query("%d <= year_built < %d and subsidized != True" % (year, year + years_per_iter))

    if not len(df):
        return

    # fees for commercial subsidy - map the vmt fee amounts designated in the vmt fee settings 
    # to residential/commercial projects based on their categorized vmt levels
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
        coffer[acct["vmt_com_acct"].add_transaction(total_fees, subaccount=1, metadata=metadata)