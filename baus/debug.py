from __future__ import print_function

import orca
import datetime
import pathlib
import traceback
import pandas as pd

# this "submodel" is purely for diagnostic debugging / development
# and shouldn't be used in production runs

@orca.step()
def debug(store, base_year, year, nodes, parcels, buildings, residential_units, households, jobs, zones, 
    acct_settings, limits_settings, coffer, logger):

    logger.debug("year={}".format(year))
    # parcels and buildings are instances of a DataFrameWrapper
    # https://udst.github.io/orca/core.html#orca.orca.DataFrameWrapper

    output_store = None
    # optional: create the 2020 input file for 2020 start (assuming this is not a 2020 start)
    if False and (year == 2020) and (base_year != 2020):
        now = datetime.datetime.now()
        h5_path = (pathlib.Path(orca.get_injectable("inputs_dir")) 
            / "basis_inputs" 
            / "parcels_buildings_agents" 
            / now.strftime("%Y_%m_%d_bayarea_2020start.h5")
        )
        output_store = pd.HDFStore(h5_path, "w")
        logger.debug("Creating {}".format(h5_path))

    nodes_columns = sorted(list(nodes.columns))
    logger.debug("nodes.columns: {}".format(nodes_columns))
    for node_col in nodes_columns:
        logger.debug("node_col {} type={}".format(node_col, nodes.column_type(node_col)))
    if output_store:
        nodes_table_columns = [
           'ave_hhsize',
           'ave_income_1500',
           'ave_income_500',
           'ave_lot_size_per_unit',
           'ave_sqft_per_unit',
           'industrial',
           'industrial_1500',
           'jobs_1500',
           'jobs_500',
           'office',
           'office_1500',
           'poor',
           'population',
           'renters',
           'residential',
           'residential_units_1500',
           'residential_units_500',
           'retail',
           'retail_1500',
           'retail_sqft_3000',
           'sfdu',
           'sum_income_3000'
        ]
        for node_col in nodes_table_columns:
            logger.debug("node_col {} type={}".format(node_col, nodes.column_type(node_col)))
        if output_store != None:
            node_store_df = nodes.to_frame(nodes_table_columns)
            logger.debug("node_store_df.dtypes:\n{}".format(node_store_df.dtypes))
            output_store["nodes"] = node_store_df

    parcels_columns = sorted(list(parcels.columns))
    logger.debug("parcels.columns: {}".format(parcels_columns))
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
        logger.debug("parcel_col {} type={}".format(parcel_col, parcels.column_type(parcel_col)))
    if output_store != None:
        parcel_store_df = parcels.to_frame(parcel_store_columns)
        logger.debug("parcel_store_df.dtypes:\n{}".format(parcel_store_df.dtypes))
        output_store["parcels"] = parcel_store_df

    buildings_columns = sorted(list(buildings.columns))
    logger.debug("buildings.columns: {}".format(buildings_columns))
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
        logger.debug("building_col {} type={}".format(building_col, buildings.column_type(building_col)))
    if output_store != None:
        buildings_store_df = buildings.to_frame(buildings_preproc_store_columns)
        logger.debug("buildings_store_df.dtypes:\n{}".format(buildings_store_df.dtypes))
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
        logger.debug("residential_unit_col {} type={}".format(residential_unit_col, residential_units.column_type(residential_unit_col)))
    if output_store != None:
        residential_units_store_df = residential_units.to_frame(residential_units_preproc_columns)
        logger.debug("residential_units_store_df.dtypes:\n{}".format(residential_units_store_df.dtypes))
        output_store["residential_units_preproc"] = residential_units_store_df

    households_columns = sorted(list(households.columns))
    logger.debug("households.columns: {}".format(households_columns))
    logger.debug("household_col {} type={}".format('node_id', households.column_type('node_id')))
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
        'move_in_year'
    ]
    for household_col in households_preproc_store_columns:
        logger.debug("household_col {} type={}".format(household_col, households.column_type(household_col)))
    if output_store != None:
        household_store_df = households.to_frame(households_preproc_store_columns)
        logger.debug("household_store_df.dtypes:\n{}".format(household_store_df.dtypes))
        output_store["households_preproc"] = household_store_df

    # these are the columns in the original store
    jobs_preproc_store_columns = [
        'sector_id',
        'empsix',
        'taz',
        'building_id',
        'move_in_year'
    ]
    for jobs_col in jobs_preproc_store_columns:
        logger.debug("jobs_col {} type={}".format(jobs_col, jobs.column_type(jobs_col)))
    if output_store != None:
        jobs_store_df = jobs.to_frame(jobs_preproc_store_columns)
        logger.debug("jobs_store_df.dtypes:\n{}".format(jobs_store_df.dtypes))
        output_store["jobs_preproc"] = jobs_store_df

    # these are the columns in the original store
    zones_store_columns = [
        'gid',
        'tract',
        'area',
        'acres'
    ]
    for zones_col in zones_store_columns:
        logger.debug("zones_col {} type={}".format(zones_col, zones.column_type(zones_col)))
    if output_store != None:
        zones_store_df = zones.to_frame(zones_store_columns)
        logger.debug("zones_store_df.dtypes:\n{}".format(zones_store_df.dtypes))
        output_store["zones"] = zones_store_df

    logger.debug("---- acct_settings ({}) ----".format(type(acct_settings)))
    for acct_key in sorted(acct_settings.keys()):
        logger.debug("{} -> {}".format(acct_key, acct_settings[acct_key]))

    logger.debug("---- limits_settings ({}) ----".format(type(limits_settings)))
    for limits_key in sorted(limits_settings.keys()):
        logger.debug("{} -> {}".format(limits_key, limits_settings[limits_key]))

    # coffer is a dictionary of Account objects
    logger.debug("---- Coffer ({}) ----".format(type(coffer)))
    for acct_name in sorted(coffer.keys()):
        logger.debug("Coffer[{}]:\n{}".format(acct_name, coffer[acct_name]))

    # close it
    if output_store:
        output_store.close()

    return
    try:
        # this may throw an exception if some variable calculations don't work
        parcels_df = parcels.to_frame()
        logger.debug("parcels len {:,} dtypes:\n{}".format(len(parcels_df), parcels_df.dtypes))
    except Exception as e:
        logger.error("parcels.to_frame() exception raised")
        logger.error(type(e))
        logger.error(e)
        logger.error(traceback.format_exc())
    return