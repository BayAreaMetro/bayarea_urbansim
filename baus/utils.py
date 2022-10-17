from __future__ import print_function
import pandas as pd
import numpy as np
import orca
import os
import sys
from urbansim_defaults.utils import _remove_developed_buildings
from urbansim.developer.developer import Developer as dev
import itertools as it


def add_buildings(buildings, new_buildings, remove_developed_buildings=True):
# similar to the function in urbansim_defaults, but it assumes you want use your own pick function
    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]

    if remove_developed_buildings:
        unplace_agents = ["households", "jobs"]
        old_buildings =  _remove_developed_buildings(old_buildings, new_buildings, unplace_agents)

    all_buildings = dev.merge(old_buildings, new_buildings)

    orca.add_table("buildings", all_buildings)


def nearest_neighbor(df1, df2):
# assume df1 and df2 each have 2 float columns specifying x and y in the same order and coordinate system and no nans  
# return the indexes from df1 that are closest to each row in df2
    from sklearn.neighbors import KDTree
    kdt = KDTree(df1.as_matrix())
    indexes = kdt.query(df2.as_matrix(), k=1, return_distance=False)
    return df1.index.values[indexes]


def groupby_random_choice(s, counts, replace=True):
# if s is a series where the index is parcel ids and the values are cities, while counts is a series 
# where the index is cities and the values are counts, you want to end up with "counts" many parcel ids from s
# this can can be thought of as grouping the dataframe "s" came from and sampling "count" number of rows from each group
    if counts.sum() == 0:
        return pd.Series()

    return pd.concat([s[s == grp].sample(cnt, replace=replace) for grp, cnt in counts[counts > 0].iteritems()])


def random_indexes(s, num, replace=False):
# pick random indexes from s without replacement
    return np.random.choice(np.repeat(s.index.values, s.values), num, replace=replace)


def round_series_match_target(s, target, fillna=np.nan):
# take a series of floating point numbers and round to integers (e.g. to a whole number households), while making sure to
# meet the given target for the sum. Lose a bit of resoltion in order to meet the target exactly
    if target == 0 or s.sum() == 0:
        return s
    r = s.fillna(fillna).round().astype('int')
    # handles rare cases where all values round to 0
    if r.sum() == 0:
        r = np.ceil(s).astype('int')
    diff = int(np.subtract(target, r.sum()))
    # diff = int(target - r.sum())
    if diff > 0:
        # replace=True allows us to add even more than we have now
        indexes = random_indexes(r, abs(diff), replace=True)
        r = r.add(pd.Series(indexes).value_counts(), fill_value=0)
    elif diff < 0:
        # this makes sure we can only subtract until all rows are zero
        indexes = random_indexes(r, abs(diff))
        r = r.sub(pd.Series(indexes).value_counts(), fill_value=0)

    assert r.sum() == target
    return r


def scale_by_target(s, target, check_close=None):
# scale (floating point ok) so that the sum of s if equal to the specified target  
# pass check_close to verify that it's within a certain range of the target
    ratio = float(target) / s.sum()
    if check_close:
        assert 1.0-check_close < ratio < 1.0+check_close
    return s * ratio


def constrained_normalization(marginals, constraint, total):
# this method increases the marginals to match the total while also meeting the matching constraint.  marginals should be
# scaled up proportionally.  it is possible that this method will fail if the sum of the constraint is less than the total
    
    assert constraint.sum() >= total

    while 1:

        # where do we exceed the total
        constrained = marginals >= constraint
        exceeds = marginals > constraint
        unconstrained = ~constrained

        num_constrained = len(constrained[constrained is True])
        num_exceeds = len(exceeds[exceeds is True])

        print("Len constrained = %d, exceeds = %d" %
              (num_constrained, num_exceeds))

        if num_exceeds == 0:
            return marginals

        marginals[constrained] = constraint[constrained]

        # scale up where unconstrained
        unconstrained_total = total - marginals[constrained].sum()
        marginals[unconstrained] *= unconstrained_total / marginals[unconstrained].sum()

        # should have scaled up
        assert np.isclose(marginals.sum(), total)


def simple_ipf(seed_matrix, col_marginals, row_marginals, tolerance=1, cnt=0):
# iterative proportional fitting where:
# seed_matrix is the totals, col_marginals are observed column marginals, and row_marginals is the same for rows
    assert np.absolute(row_marginals.sum() - col_marginals.sum()) < 5.0

    # most numpy/pandas combinations will perform this conversion automatically, but explicit is safer - see PR #99
    if isinstance(col_marginals, pd.Series):
        col_marginals = col_marginals.values

    # first normalize on columns
    ratios = col_marginals / seed_matrix.sum(axis=0)

    seed_matrix *= ratios
    closeness = np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum()
    assert np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum() < .01
    print("row closeness", closeness)
    if closeness < tolerance:
        return seed_matrix

    # first normalize on rows
    ratios = np.array(row_marginals / seed_matrix.sum(axis=1))
    ratios[row_marginals == 0] = 0
    seed_matrix = seed_matrix * ratios.reshape((ratios.size, 1))
    assert np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum() < .01
    closeness = np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum()
    print("col closeness", closeness)
    if closeness < tolerance:
        return seed_matrix

    if cnt >= 50:
        return seed_matrix

    return simple_ipf(seed_matrix, col_marginals, row_marginals,
                      tolerance, cnt+1)


def divide_series(a_tuple, variable):
    s = get_outcome_df(a_tuple[0])[variable]
    s1 = get_outcome_df(a_tuple[1])[variable]
    s2 = s1 / s
    s2.name = str(a_tuple[1]) + '/' + str(a_tuple[0])
    return s2


def get_combinations(nparray):
    return pd.Series(list(it.combinations(np.unique(nparray), 2)))
 

def profit_to_prob_func(df):
# a custom profit to probability function where we test the combination of different metrics like return on cost and raw profit
# clip since we still might build negative profit buildings (when we're subsidizing them) and choice doesn't allow negative
# probability options
    max_profit = df.max_profit.clip(1)

    factor = float(orca.get_injectable("settings")["profit_vs_return_on_cost_combination_factor"])

    df['return_on_cost'] = max_profit / df.total_cost

    # now we're going to make two pdfs and weight them
    ROC_p = df.return_on_cost.values / df.return_on_cost.sum()
    profit_p = max_profit / max_profit.sum()
    p = 1.0 * ROC_p + factor * profit_p

    return p / p.sum()


@orca.injectable(autocall=False)
def add_extra_columns_func(df):
    df['source'] = 'developer_model'

    for col in ["residential_price", "non_residential_rent"]:
        df[col] = 0

    if "deed_restricted_units" not in df.columns:
        df["deed_restricted_units"] = 0
    else:
        print("Number of deed restricted units built = %d" %
              df.deed_restricted_units.sum())
    df["preserved_units"] = 0.0

    if "inclusionary_units" not in df.columns:
        df["inclusionary_units"] = 0
    else:
        print("Number of inclusionary units built = %d" %
              df.inclusionary_units.sum())

    if "subsidized_units" not in df.columns:
        df["subsidized_units"] = 0
    else:
        print("Number of subsidized units built = %d" %
              df.subsidized_units.sum())

    df["redfin_sale_year"] = 2012
    df["redfin_sale_price"] = np.nan

    if "residential_units" not in df:
        df["residential_units"] = 0

    if "parcel_size" not in df:
        df["parcel_size"] = orca.get_table("parcels").parcel_size.loc[df.parcel_id]

    if orca.is_injectable("year") and "year_built" not in df:
        df["year_built"] = orca.get_injectable("year")

    if orca.is_injectable("form_to_btype_func") and "building_type" not in df:
        form_to_btype_func = orca.get_injectable("form_to_btype_func")
        df["building_type"] = df.apply(form_to_btype_func, axis=1)

    return df


@orca.injectable(autocall=False)
def supply_and_demand_multiplier_func(demand, supply):
    s = demand / supply
    settings = orca.get_injectable('settings')
    print("Number of submarkets where demand exceeds supply:", len(s[s > 1.0]))
    # print "Raw relationship of supply and demand\n", s.describe()
    supply_correction = settings["price_equilibration"]
    clip_change_high = supply_correction["kwargs"]["clip_change_high"]
    t = s
    t -= 1.0
    t = t / t.max() * (clip_change_high-1)
    t += 1.0
    s.loc[s > 1.0] = t.loc[s > 1.0]
    return s, (s <= 1.0).all()


@orca.injectable(autocall=False)
def form_to_btype_func(building):
    # map a specific building that we build to a specific building type
    mapping = orca.get_injectable('mapping')
    form = building.form
    dua = building.residential_units / (building.parcel_size / 43560.0)
    # precise mapping of form to building type for residential
    if form is None or form == "residential":
        if dua < 16:
            return "HS"
        elif dua < 32:
            return "HT"
        return "HM"
    return mapping["form_to_btype"][form][0]