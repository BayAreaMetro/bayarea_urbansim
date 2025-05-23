from __future__ import print_function

import pandas as pd
import numpy as np
import orca
import os
import sys
from urbansim_defaults.utils import _remove_developed_buildings
from urbansim.developer.developer import Developer as dev
import itertools as it

import logging

# Get a logger specific to this module
logger = logging.getLogger(__name__)

#####################
# UTILITY FUNCTIONS
#####################


# This is really simple and basic (hackish) code to solve a very important
# problem.  I'm calling an orca step and want to serialize the tables that
# are passed in so that I can add code to this orca step and test it quickly
# without running all the models that came before, which takes many minutes.
#
# If the passed in outhdf does not exist, all the DataFarmeWrappers which
# have been passed have their DataFrames saved to the hdf5.  If the file
# does exist they all get restored, and as a byproduct their types are
# changed from DataFrameWrappers to DataFrames.  This type change is a
# major drawback of the current code, as is the fact that other non-
# DataFrame types are not serialized, but it's a start.  I plan to bring
# this problem up on the UrbanSim coders call the next time we meet.
#
#    Sample code:
#
#    d = save_and_restore_state(locals())
#    for k in d.keys():
#        locals()[k].local = d[k]
#
def save_and_restore_state(in_d, outhdf="save_state.h5"):
    if os.path.exists(outhdf):
        # the state exists - load it back into the locals
        store = pd.HDFStore(outhdf)
        out_d = {}
        for table_name in store:
            print("Restoring", table_name)
            out_d[table_name[1:]] = store[table_name]
        return out_d

    # the state doesn't exist and thus needs to be saved
    store = pd.HDFStore(outhdf, "w")
    for table_name, table in in_d.items():
        try:
            table = table.local
        except Exception as e:
            # not a dataframe wrapper
            continue
        print("Saving", table_name)
        store[table_name] = table
    store.close()
    sys.exit(0)


# similar to the function in urbansim_defaults, except it assumes you want
# to use your own pick function
def add_buildings(buildings, new_buildings,
                  remove_developed_buildings=True):

    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]

    if remove_developed_buildings:
        unplace_agents = ["households", "jobs"]
        old_buildings = \
            _remove_developed_buildings(old_buildings, new_buildings,
                                        unplace_agents)

    all_buildings = dev.merge(old_buildings, new_buildings)

    orca.add_table("buildings", all_buildings)


# assume df1 and df2 each have 2 float columns specifying x and y
# in the same order and coordinate system and no nans.  returns the indexes
# from df1 that are closest to each row in df2
def nearest_neighbor(df1, df2):
    from sklearn.neighbors import KDTree
    kdt = KDTree(df1.as_matrix())
    indexes = kdt.query(df2.as_matrix(), k=1, return_distance=False)
    return df1.index.values[indexes]


# need to reindex from geom id to the id used on parcels
def geom_id_to_parcel_id(df, parcels):
    s = parcels.geom_id  # get geom_id
    s = pd.Series(s.index, index=s.values)  # invert series
    df["new_index"] = s.loc[df.index]  # get right parcel_id for each geom_id
    df = df.dropna(subset=["new_index"])
    df["new_index"] = df.new_index.astype('int')
    df = df.set_index("new_index", drop=True)
    df.index.name = "parcel_id"
    return df


def parcel_id_to_geom_id(s):
    parcels = orca.get_table("parcels")
    g = parcels.geom_id  # get geom_id
    return pd.Series(g.loc[s.values].values, index=s.index)


# This is best described by example. Imagine s is a series where the
# index is parcel ids and the values are cities, while counts is a
# series where the index is cities and the values are counts.  You
# want to end up with "counts" many parcel ids from s (like 40 from
# Oakland, 60 from SF, and 20 from San Jose).  This can be
# thought of as grouping the dataframe "s" came from and sampling
# count number of rows from each group.  I mean, you group the
# dataframe and then counts gives you the count you want to sample
# from each group.
def groupby_random_choice(s, counts, replace=True):
    if counts.sum() == 0:
        return pd.Series()

    return pd.concat([
        s[s == grp].sample(cnt, replace=replace)
        for grp, cnt in counts[counts > 0].iteritems()
    ])


# pick random indexes from s without replacement
def random_indexes(s, num, replace=False):
    return np.random.choice(
        np.repeat(s.index.values, s.values),
        num,
        replace=replace)


# This method takes a series of floating point numbers, rounds to
# integers (e.g. to while number households), while making sure to
# meet the given target for the sum.  We're obviously going to lose
# some resolution on the distrbution implied by s in order to meet
# the target exactly
def round_series_match_target(s, target, fillna=np.nan):
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


# scales (floating point ok) so that the sum of s if equal to
# the specified target - pass check_close to verify that it's
# within a certain range of the target
def scale_by_target(s, target, check_close=None):
    ratio = float(target) / s.sum()
    if check_close:
        assert 1.0-check_close < ratio < 1.0+check_close
    return s * ratio


def constrained_normalization(marginals, constraint, total):
    # this method increases the marginals to match the total while
    # also meeting the matching constraint.  marginals should be
    # scaled up proportionally.  it is possible that this method
    # will fail if the sum of the constraint is less than the total

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
        marginals[unconstrained] *= \
            unconstrained_total / marginals[unconstrained].sum()

        # should have scaled up
        assert np.isclose(marginals.sum(), total)


# this should be fairly self explanitory if you know ipf
# seed_matrix is your best bet at the totals, col_marginals are
# observed column marginals and row_marginals is the same for rows
def simple_ipf(seed_matrix, col_marginals, row_marginals, tolerance=1, cnt=0):
    assert np.absolute(row_marginals.sum() - col_marginals.sum()) < 5.0

    # most numpy/pandas combinations will perform this conversion
    # automatically, but explicit is safer - see PR #99
    if isinstance(col_marginals, pd.Series):
        col_marginals = col_marginals.values

    # first normalize on columns
    ratios = col_marginals / seed_matrix.sum(axis=0)

    seed_matrix *= ratios
    closeness = np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum()
    assert np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum() < .01
    logger.debug("row closeness {}".format(closeness))
    if closeness < tolerance:
        return seed_matrix

    # first normalize on rows
    ratios = np.array(row_marginals / seed_matrix.sum(axis=1))
    ratios[row_marginals == 0] = 0
    seed_matrix = seed_matrix * ratios.reshape((ratios.size, 1))
    assert np.absolute(row_marginals - seed_matrix.sum(axis=1)).sum() < .01
    closeness = np.absolute(col_marginals - seed_matrix.sum(axis=0)).sum()
    logger.debug("col closeness {}".format(closeness))
    if closeness < tolerance:
        return seed_matrix

    if cnt >= 50:
        return seed_matrix

    return simple_ipf(seed_matrix, col_marginals, row_marginals,
                      tolerance, cnt+1)


def subtract_base_year_urban_footprint(run_number):
    base_year_filename = \
        'runs/run{}_urban_footprint_summary_summaries_{}.csv'.\
        format(run_number, 2010)
    bdf = pd.read_csv(base_year_filename, index_col=0)
    outcome_year_filename = \
        'runs/run{}_urban_footprint_summary_summaries_{}.csv'.\
        format(run_number, 2040)
    odf = pd.read_csv(outcome_year_filename, index_col=0)
    sdf = odf - bdf
    sdf.to_csv(
        'runs/run{}_urban_footprint_subtracted_summaries_{}.csv'
        .format(run_number, 2040))


def pipeline_filtering(input_pipeline, list_filter_criteria):
    """Filters a DataFrame of development projects based on specified criteria.

    Args:
        input_pipeline (pd.DataFrame): The DataFrame containing development projects.
        filter_criteria (dict): A dictionary where keys are column names and values are tuples
            containing the comparison operator and the value to compare against.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """

    key_numerics = ['non_residential_sqft', 'residential_units']

    print('Starting length of the pipeline: {:,}'.format(len(input_pipeline)))

    for filter_criteria in list_filter_criteria:
        filter_components = []
        for key, operator_value in filter_criteria.items():
            
            # name key is just matched to a county identifier so no conditions found in values there
            if not key == 'name':
                 
                value = operator_value['value']
                operator = operator_value['operator']
                
                # Generate query expression list from components
                lst_conditions = [
                    f"{key}{operator}" +
                    f'"{value}"' if isinstance(
                        value, str) else f"{key}{operator}{value}"
                ]

                # Generate query string from query list
                str_conditions = "".join(lst_conditions)
                filter_components.append(str_conditions)

        # Concatenate component filters
        str_full_query = " & ".join(filter_components)

        # Create mask of matching records from component filters - to drop
        drop_mask = input_pipeline.eval(str_full_query)

        if len(drop_mask.value_counts()) > 1:
            drop_summary = input_pipeline[drop_mask][key_numerics].sum()
            series_string = f"\tRecords Lost: {'; '.join([f'{key}: {value:,.0f}' for key, value in drop_summary.items()])}"

            print(
                f'Applying filter: `{str_full_query}`\n',
                f'\tRemoving {drop_mask.value_counts()[True]} records from pipeline\n',
                #f'\tRecords lost:\n\t{input_pipeline[drop_mask][key_numerics].sum()}',
                series_string)
            
            # applying to source data
            input_pipeline = input_pipeline[~drop_mask]

        else:
            print('Filters match no records. Keeping all pipeline records.')

    # Return resulting pipeline
    return input_pipeline