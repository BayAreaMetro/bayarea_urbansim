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



def policy_modifications_of_profit(feasibility, parcels):
    # modify the max_profit column of the feasibility dataframe

    print("Making policy modifications to profitability")

    # add parcel unit-based fees
    units = feasibility[('residential', 'residential_sqft')] / \
        parcels.ave_sqft_per_unit
    fees = (units * parcels.fees_per_unit).fillna(0)
    print("Sum of residential fees: ", fees.sum())

    feasibility[("residential", "fees")] = fees
    feasibility[("residential", "max_profit")] -= fees

    #  now non-residential fees per sqft
    for use in ["retail", "office"]:

        if (use, 'non_residential_sqft') not in feasibility.columns:
            continue

        sqft = feasibility[(use, 'non_residential_sqft')]
        fees = (sqft * parcels.fees_per_sqft).fillna(0)
        print("Sum of non-residential fees (%s): %.0f" % (use, fees.sum()))

        feasibility[(use, "fees")] = fees
        feasibility[(use, "max_profit")] -= fees

    return feasibility


def inclusionary_housing_revenue_reduction(inclusionary_zoning, feasibility, units):
    # compute the reduction in revenue from a project due to inclustionary housing

    # deed restriction is done by household size, but this requires 
    # a better representation of what household size is in which unit type
    print("Computing adjustments due to inclusionary housing")

    households = orca.get_table("households")
    buildings = orca.get_table("buildings")
    parcels_geography = orca.get_table("parcels_geography")

    h = orca.merge_tables("households", [households, buildings, parcels_geography], columns=["juris_name","income"])
    
    # AMI
    AMI = h.groupby(h.gg).income.quantile(.5)
    # percent of AMI used
    pct_of_AMI = .9
    # max amount a household pays per year
    amt_per_year = .33
    # monthly condo fee
    monthly_condo_fee = 250
    # monthy affordable payment calculation
    monthly_affordable_payment = AMI * pct_of_AMI * amt_per_year/12 - monthly_condo_fee

    def value_can_afford(monthly_payment):
        # 10 year average freddie mac interest rate
        ten_year_average_interest = .055
        return np.npv(ten_year_average_interest/12, [monthly_payment]*30*12)

    value_can_afford = {k: value_can_afford(v) for k, v in monthly_affordable_payment.to_dict().items()}
    value_can_afford = pd.Series(value_can_afford)

    # account for interest and property taxes
    interest_and_prop_taxes = .013
    value_can_afford /= 1+interest_and_prop_taxes

    # there's a lot more nuance to inclusionary percentages than this
    # e.g. specific neighborhoods get specific amounts 

    # calculate revenue reduction
    pct_inclusionary = inclusionary_zoning.to_frame().inclusionary_pct
    geog = parcels_geography.gg.loc[feasibility.index]
    pct_affordable = geog.map(pct_inclusionary).fillna(0)
    value_can_afford = geog.map(value_can_afford)

    num_affordable_units = (units * pct_affordable).fillna(0).astype("int")
    ave_price_per_unit = feasibility[('residential', 'building_revenue')] / units
    revenue_diff_per_unit = (ave_price_per_unit - value_can_afford).fillna(0)
    revenue_reduction = revenue_diff_per_unit * num_affordable_units

    return revenue_reduction, num_affordable_units


@orca.step()
def policy_modifications_of_profit_inclusionary_zoning(feasibility, units, parcels, inclusionary_zoning, 
                                                       inclusionary_housing_revenue_reduction):
    #  adds inclusionary housing reduction in revenue
    revenue_reduction, num_affordable_units = inclusionary_housing_revenue_reduction(feasibility, units)

    assert np.all(num_affordable_units <= units.fillna(0))

    print("Describe of inclusionary revenue reduction:\n", revenue_reduction[revenue_reduction > 0].describe())
    print("Describe of number of affordable units:\n", num_affordable_units[num_affordable_units > 0].describe())

    feasibility[("residential", "policy_based_revenue_reduction")] = revenue_reduction
    feasibility[("residential", "max_profit")]                    -= revenue_reduction
    feasibility[("residential", "deed_restricted_units")]          = num_affordable_units
    feasibility[("residential", "inclusionary_units")]             = num_affordable_units

    return feasibility


@orca.step()
def policy_modifications_of_profit_sb743(feasibility, parcels, sb743):    
    sb_743_settings = sb_743.to_frame()

    pct_modifications = feasibility[("residential", "vmt_res_cat")].map(sb743_settings.pcts) + 1
    print("Modifying profit for SB743:\n", pct_modifications.describe())

    feasibility[("residential", "max_profit")] *= pct_modifications
    return feasibility


@orca.step()
def policy_modifications_of_profit_land_value_tax(feasibility, parcels, land_value_tax):    
    lvt = land_value_tax.to_frame()

    pcts = lvt.pcts
    # need to bound the breaks with a reasonable low and high goalpost
    breaks = [-1]+lvt.breaks+[2]
    bins = (pct, breaks)

    pzc = orca.get_table("parcels_zoning_calculations")
    s = pzc.zoned_build_ratio
    # map the breakpoints to the pcts
    pct_modifications = pd.cut(s, breaks, labels=pcts).astype('float') + 1
    # if some parcels got skipped, fill them in with no modification
    pct_modifications = pct_modifications.reindex(pzc.index).fillna(1.0)

    print("Modifying profit for Land Value Tax:\n", pct_modifications.describe())
    feasibility[("residential", "max_profit")] *= pct_modifications

    return feasibility


@orca.step()
def policy_modifications_of_profit_parking_requirements(feasibility, parcels, parking_reqs):
    pr = parking_reqs.to_frame()
    formula = pr.profitability_adjustment_formula
    parcels_geography = orca.get_table("parcels_geography")
    pct_modifications = parcels_geography.local.eval(formula)
    pct_modifications += 1.0

    print("Modifying profit for %s:\n" % policy["name"],pct_modifications.describe())
    print("Formula: \n{}".format(formula))

    feasibility[("residential", "max_profit")] *= pct_modifications

    print("There are %d affordable units if all feasible projects are built" %
          feasibility[("residential", "deed_restricted_units")].sum())

    return feasibility


@orca.step()
def policy_modifications_of_profit_ceqa_reform(feasibility, parcels, ceqa_reform):
    cr = ceqa_reform.to_frame()
    formula = cr.profitability_adjustment_formula
    pct_modifications = parcels_geography.local.eval(formula)
    pct_modifications += 1.0

    print("Modifying profit for %s:\n" % "ceqa_reform", pct_modifications.describe())
    print("Formula: \n{}".format(formula))

    feasibility[("residential", "max_profit")] *= pct_modifications

    print("There are %d affordable units if all feasible projects are built" %
          feasibility[("residential", "deed_restricted_units")].sum())

    return feasibility
