from __future__ import print_function

import orca
import traceback

# this "submodel" is purely for diagnostic debugging / development
# and shouldn't be used in production runs

@orca.step()
def debug(year): # , nodes, parcels, buildings):
    print("year={}".format(year))
    return
    # parcels and buildings are instances of a DataFrameWrapper
    # https://udst.github.io/orca/core.html#orca.orca.DataFrameWrapper

    print("nodes.columns: {}".format(nodes.columns))
    for node_col in nodes.columns:
        print("node_col {} type={}".format(node_col, nodes.column_type(node_col)))

    print("parcels.columns: {}".format(parcels.columns))
    for parcel_col in parcels.columns:
        print("parcel_col {} type={}".format(parcel_col, parcels.column_type(parcel_col)))

    print("buildings.columns: {}".format(buildings.columns))
    for building_col in buildings.columns:
        print("building_col {} type={}".format(building_col, buildings.column_type(building_col)))

    return
    try:
        # this may throw an exception if some variable calculations don't work
        parcels_df = parcels.to_frame()
        print("parcels len {:,} dtypes:\n{}".format(len(parcels_df), parcels_df.dtypes))
    except Exception as e:
        print("parcels.to_frame() exception raised")
        print(type(e))
        print(e)
        print(traceback.format_exc())
    return