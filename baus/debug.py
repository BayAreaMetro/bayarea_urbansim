from __future__ import print_function

import orca
import traceback

# this "submodel" is purely for diagnostic debugging / development
# and shouldn't be used in production runs

@orca.step()
def debug(year, nodes, parcels, buildings):
    print("year={}".format(year))
    # parcels and buildings are instances of a DataFrameWrapper
    # https://udst.github.io/orca/core.html#orca.orca.DataFrameWrapper

    nodes_columns = sorted(list(nodes.columns))
    print("nodes.columns: {}".format(nodes_columns))
    for node_col in nodes_columns:
        print("node_col {} type={}".format(node_col, nodes.column_type(node_col)))

    parcels_columns = sorted(list(parcels.columns))
    print("parcels.columns: {}".format(parcels_columns))
    for parcel_col in parcels_columns:
        print("parcel_col {} type={}".format(parcel_col, parcels.column_type(parcel_col)))

    buildings_columns = sorted(list(buildings.columns))
    print("buildings.columns: {}".format(buildings_columns))
    for building_col in buildings_columns:
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