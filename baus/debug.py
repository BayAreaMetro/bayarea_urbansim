from __future__ import print_function

import orca
import datetime
import pathlib
import traceback
import pandas as pd

# this "submodel" is purely for diagnostic debugging / development
# and shouldn't be used in production runs

@orca.step()
def debug(year, nodes, parcels, buildings, residential_units, households, jobs, zones):
    print("year={}".format(year))
    # parcels and buildings are instances of a DataFrameWrapper
    # https://udst.github.io/orca/core.html#orca.orca.DataFrameWrapper

    nodes_columns = sorted(list(nodes.columns))
    print("nodes.columns: {}".format(nodes_columns))
    for node_col in nodes_columns:
        print("node_col {} type={}".format(node_col, nodes.column_type(node_col)))

    output_store = None
    if year == 2020:
        now = datetime.datetime.now()
        h5_path = (pathlib.Path(orca.get_injectable("inputs_dir")) 
            / "basis_inputs" 
            / "parcels_buildings_agents" 
            / now.strftime("%Y_%m_%d_bayarea_2020start.h5")
        )
        output_store = pd.HDFStore(h5_path, "w")
        print("Saving {}".format(h5_path))

    parcels_columns = sorted(list(parcels.columns))
    print("parcels.columns: {}".format(parcels_columns))
    # these are the columns in the original store
    parcel_store_columns = [
        'development_type_id',
        'land_value',
        'acres',
        'county_id',
        'zone_id',
        'proportion_undevelopable',
        'tax_exempt_status',
        'apn',
        'parcel_id_local',
        'geom_id',
        'imputation_flag',
        'x',
        'y',
        'shape_area'
    ]
    for parcel_col in parcel_store_columns:
        print("parcel_col {} type={}".format(parcel_col, parcels.column_type(parcel_col)))
    if output_store != None:
        parcel_store_df = parcels.to_frame(parcel_store_columns)
        print("parcel_store_df.dtypes:\n{}".format(parcel_store_df.dtypes))
        output_store["parcels"] = parcel_store_df

    buildings_columns = sorted(list(buildings.columns))
    print("buildings.columns: {}".format(buildings_columns))
    # these are the columns in the original store
    buildings_preproc_store_columns = [
        'parcel_id',
        'residential_units',
        'residential_sqft',
        'non_residential_sqft',
        'building_sqft',
        'stories',
        'year_built',
        'redfin_sale_price',
        'redfin_sale_year',
        'source',
        'preserved_units',
        'inclusionary_units',
        'subsidized_units',
        'building_type',
        'residential_price',
        'non_residential_rent',
        'deed_restricted_units',
    ]
    for building_col in buildings_preproc_store_columns:
        print("building_col {} type={}".format(building_col, buildings.column_type(building_col)))
    if output_store != None:
        buildings_store_df = buildings.to_frame(buildings_preproc_store_columns)
        print("buildings_store_df.dtypes:\n{}".format(buildings_store_df.dtypes))
        output_store["buildings_preproc"] = buildings_store_df

    residential_units_preproc_columns = [
        'unit_residential_price',
        'unit_residential_rent',
        'num_units',
        'building_id',
        'unit_num',
        'deed_restricted',
        'tenure',
    ]
    for residential_unit_col in residential_units_preproc_columns:
        print("residential_unit_col {} type={}".format(residential_unit_col, residential_units.column_type(residential_unit_col)))
    if output_store != None:
        residential_units_store_df = residential_units.to_frame(residential_units_preproc_columns)
        print("residential_units_store_df.dtypes:\n{}".format(residential_units_store_df.dtypes))
        output_store["residential_units_preproc"] = residential_units_store_df

    # these are the columns in the original store
    households_preproc_store_columns = [
        'base_income_octile',
        'base_income_quartile',
        'bldgsz',
        'bucketbin',
        'building_id',
        'h0004',
        'h0511',
        'h1215',
        'h1617',
        'h1824',
        'h2534',
        'h3549',
        'h5064',
        'h6579',
        'h80up',
        'hadkids',
        'hadnwst',
        'hadwpst',
        'hfamily',
        'hhagecat',
        'hht',
        'hinccat1',
        'hinccat2',
        'hmultiunit',
        'hnoccat',
        'hnwork',
        'household_id',
        'hownrent',
        'hpresch',
        'hretire',
        'hschdriv',
        'hschpred',
        'hsizecat',
        'htypdwel',
        'hunittype',
        'huniv',
        'hwork_f',
        'hwork_p',
        'hworkers',
        'hwrkrcat',
        'income',
        'noc',
        'originalpuma',
        'persons',
        'puma5',
        'serialno',
        'taz',
        'tenure',
        'unittype',
        'vehicl',
        'unit_num',
        'unit_id',
    ]
    for household_col in households_preproc_store_columns:
        print("household_col {} type={}".format(household_col, households.column_type(household_col)))
    if output_store != None:
        household_store_df = households.to_frame(households_preproc_store_columns)
        print("household_store_df.dtypes:\n{}".format(household_store_df.dtypes))
        output_store["households_preproc"] = household_store_df
    
    # these are the columns in the original store
    jobs_preproc_store_columns = [
        'sector_id',
        'empsix',
        'taz',
        'building_id'
    ]
    for jobs_col in jobs_preproc_store_columns:
        print("jobs_col {} type={}".format(jobs_col, jobs.column_type(jobs_col)))
    if output_store != None:
        jobs_store_df = jobs.to_frame(jobs_preproc_store_columns)
        print("jobs_store_df.dtypes:\n{}".format(jobs_store_df.dtypes))
        output_store["jobs_preproc"] = jobs_store_df

    # these are the columns in the original store
    zones_store_columns = [
        'gid',
        'tract',
        'area',
        'acres'
    ]
    for zones_col in zones_store_columns:
        print("zones_col {} type={}".format(zones_col, zones.column_type(zones_col)))
    if output_store != None:
        zones_store_df = zones.to_frame(zones_store_columns)
        print("zones_store_df.dtypes:\n{}".format(zones_store_df.dtypes))
        output_store["zones"] = zones_store_df

    
    # close it
    if output_store:
        output_store.close()

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