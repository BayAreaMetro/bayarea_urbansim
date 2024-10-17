USAGE = """
    Example call: python zoning_mapping_QAQC.py NoProject M:/urban_modeling/baus/PBA50Plus/PBA50Plus_NoProject/PBA50Plus_NoProject_v13_zn_revisit_ugb county
    
    For a given run, this script:
        - pulls base zoning and zoningmods inputs
        - attaches them to parcel spatial data
        - writes out the data for visual QAQC, by county or superdistrict for easier loading into ArcGIS/Tableau.
"""


import pandas as pd 
import geopandas as gpd
import fiona
import os, sys, argparse, logging, yaml, shutil

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description = USAGE,
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('scenario', type=str, choices=['NoProject','DBP'], help='NoProject or DBP')
    parser.add_argument('run_dir', type=str)
    parser.add_argument('export_geo', type=str, choices=['county','superdistrict'], help='write out spatial files by county or superdistrict')
    my_args = parser.parse_args()

    #### Set up folder and file locations
    # make dir to write out the QAQC files if not exist
    QAQC_DIR = os.path.join(my_args.run_dir, 'zoningQAQC')
    if not os.path.exists(QAQC_DIR):
        os.makedirs(QAQC_DIR)
    # else:
    #     shutil.rmtree(QAQC_DIR)           # Removes all the subdirectories
    #     os.makedirs(QAQC_DIR)

    # input files
    RUN_FULL_NAME = os.path.basename(os.path.normpath(my_args.run_dir))
    RUNSETUP_FILE = os.path.join(my_args.run_dir, 'run_setup_{}.yaml'.format(RUN_FULL_NAME))
    with open(RUNSETUP_FILE) as runSetupYaml:
        try:
            runSetup = yaml.safe_load(runSetupYaml)
            inputs_dir = runSetup['inputs_dir']

            P10_GEO_PATH = os.path.join(inputs_dir, "basis_inputs/crosswalks", runSetup['parcels_geography_file'])
            PARCEL_GEOGRAPHY_COLS = runSetup['parcels_geography_cols']
            
            BASE_ZONING_LOOKUP_PATH = os.path.join(inputs_dir, "basis_inputs/zoning", runSetup['zoning_lookup_file']) 
            BASE_ZONING_PARCEL_PATH = os.path.join(inputs_dir, "basis_inputs/zoning", runSetup['zoning_file'])

            UPZONING_PATH = os.path.join(inputs_dir, "plan_strategies", runSetup['zoning_mods_file'])
            ZONINGMODCAT_COLS = runSetup['zoningmodcat_cols']

        except yaml.YAMLError as exc:
            print(exc)

    # other helper files
    P10_DATASET_PATH = r'C:\Users\{}\Box\baydata\smelt\2020 07 16\smelt.gdb'.format(os.getenv('USERNAME'))

    # parcel - TAZ1454 crosswalk
    P10_ZONE_PATH = r'M:\urban_modeling\baus\BAUS Inputs\basis_inputs\crosswalks\2020_08_17_parcel_to_taz1454sub.csv'
    # TAZ1454 - SD crosswalk
    ZONE_SD_PATH = r'M:\Crosswalks\TAZ1454Superdistrict34.csv'


    #### Setup logging ############################################################################################################
    LOG_FILE = os.path.join(QAQC_DIR, "zoning_mapping_QAQC_{}.log")
    # create logger
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel('INFO')

    # console handler
    ch = logging.StreamHandler()
    ch.setLevel('INFO')
    ch.setFormatter(logging.Formatter('%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    LOGGER.addHandler(ch)
    # file handlers
    fh = logging.FileHandler(LOG_FILE.format("info"), mode='w')
    fh.setLevel('INFO')
    fh.setFormatter(logging.Formatter('%(message)s'))
    LOGGER.addHandler(fh)

    LOGGER.info('run name: {}'.format(RUN_FULL_NAME))
    LOGGER.info('run dir: {}'.format(my_args.run_dir))
    LOGGER.info('run scenario: {}'.format(my_args.scenario))

    #### parcel data and crosswalks
    # p10 spatial data
    P10_DATASET_PATH = r'C:\Users\{}\Box\baydata\smelt\2020 07 16\smelt.gdb'.format(os.getenv('USERNAME'))
    LOGGER.info('p10 spatial data: {}'.format(P10_DATASET_PATH))
    p10_layers = fiona.listlayers(P10_DATASET_PATH)
    LOGGER.info('layers: {}'.format(p10_layers))

    p10_geo = gpd.read_file(P10_DATASET_PATH, driver='fileGDB', layer='p10_pba50')
    LOGGER.info('loaded p10 geo data, {} rows, {} unique PARCEL_ID'.format(len(p10_geo), p10_geo['PARCEL_ID'].nunique()))

    # parcel - SD crosswalk
    LOGGER.info('load parcel - TAZ data from: {}'.format(P10_ZONE_PATH))
    parcels_zone = pd.read_csv(P10_ZONE_PATH)
    LOGGER.info('{} rows, {} unique PARCEL_ID'.format(len(parcels_zone), parcels_zone['PARCEL_ID'].nunique()))
    LOGGER.info(parcels_zone.head(2))
    
    LOGGER.info('load zone - SD data from {}'.format(ZONE_SD_PATH))
    zone_SD = pd.read_csv(ZONE_SD_PATH)
    LOGGER.info('{} rows'.format(len(zone_SD)))
    LOGGER.info(zone_SD.head(2))

    # merge
    parcel_SD = pd.merge(
        parcels_zone[['PARCEL_ID', 'ZONE_ID', 'county']].rename({'ZONE_ID': 'TAZ1454'}, axis=1),
        zone_SD.rename({'TAZ': 'TAZ1454'}, axis=1),
        on='TAZ1454',
        how='left'
        )
    LOGGER.info('merged, parcel-SD crosswalk has {} rows, {} unique PARCEL_ID'.format(len(parcel_SD), parcel_SD['PARCEL_ID'].nunique()))
    LOGGER.info(parcel_SD.head(2))

    # parcel_geo crosswalk
    LOGGER.info('load parcels_geogarphy file: {}'.format(P10_GEO_PATH))
    parcels_geo = pd.read_csv(P10_GEO_PATH)
    LOGGER.info('{} rows, {} unique PARCEL_ID'.format(len(parcels_geo), parcels_geo['PARCEL_ID'].nunique()))
    LOGGER.info(parcels_geo[['PARCEL_ID']].dtypes)
    LOGGER.info(parcels_geo.head())

    #### upzoning
    # first, construct zoningmodcat for upzoning lookup
    for col in PARCEL_GEOGRAPHY_COLS:
        # keep PBA50 format
        parcels_geo[col] = parcels_geo[col].astype(str).str.lower()

    LOGGER.info('constructing zoningmodscat using: {}'.format(ZONINGMODCAT_COLS))
    parcels_geo['zoningmodcat'] = ''
    for col in ZONINGMODCAT_COLS:
        parcels_geo['zoningmodcat'] = parcels_geo['zoningmodcat'] + parcels_geo[col]
    LOGGER.info("zoningmodcat unique count: {}".format(parcels_geo['zoningmodcat'].nunique()))
    LOGGER.info(parcels_geo['zoningmodcat'].unique())

    # upzoning input
    LOGGER.info('load zoningmods file: {}'.format(UPZONING_PATH))
    upzoning_lookup = pd.read_csv(UPZONING_PATH)
    LOGGER.info('zoningmods file has {} rows, {} unique zoningmodcat'.format(len(upzoning_lookup), upzoning_lookup['zoningmodcat'].nunique()))
    LOGGER.info(upzoning_lookup.head(2))

    # check - should have same number of unique zoningmodcat
    assert(len(upzoning_lookup) == parcels_geo['zoningmodcat'].nunique())

    # join to parcels
    upzoning_parcel = pd.merge(
        parcels_geo[['PARCEL_ID', 'zoningmodcat']],
        upzoning_lookup[['zoningmodcat', 'dua_up', 'dua_down', 'far_up', 'far_down', 'add_bldg', 'drop_bldg'] + ZONINGMODCAT_COLS],
        on='zoningmodcat',
        how='left'
    )
    LOGGER.info('merge zoningmods to parcels, {} rows'.format(len(upzoning_parcel)))

    #### base zoning
    LOGGER.info('load base zoning data: zoning_lookup: {}, zoning_parcel: {}'.format(BASE_ZONING_LOOKUP_PATH, BASE_ZONING_PARCEL_PATH))
    zoning_lookup = pd.read_csv(BASE_ZONING_LOOKUP_PATH)
    zoning_parcel = pd.read_csv(BASE_ZONING_PARCEL_PATH)
    LOGGER.info('zoning_lookup has {} rows'.format(len(zoning_lookup)))
    LOGGER.info(zoning_lookup.head())
    LOGGER.info('zoning_parcel has {} rows'.format(len(zoning_parcel)))
    LOGGER.info(zoning_parcel.head())

    # replicate how UrbanSim understands basezoning
    basezoning = pd.merge(
        zoning_parcel[['PARCEL_ID', 'zoning_id']],
        zoning_lookup.rename({'id': 'zoning_id'}, axis=1),
        on='zoning_id',
        how='left'
        )
    LOGGER.info('merged parcel base zoning data has {} rows'.format(len(basezoning)))

    GROSS_AVE_UNIT_SIZE = 1000.0
    PARCEL_USE_EFFICIENCY = .8
    HEIGHT_PER_STORY = 12.0

    basezoning['max_dua_from_far'] = basezoning['max_far'] * 43560 / GROSS_AVE_UNIT_SIZE
    basezoning['max_far_from_height'] = (basezoning['max_height'] / HEIGHT_PER_STORY) * PARCEL_USE_EFFICIENCY
    basezoning['max_dua_from_height'] = basezoning['max_far_from_height'] * 43560 / GROSS_AVE_UNIT_SIZE

    basezoning['base_dua'] = pd.concat([basezoning['max_dua'], basezoning['max_dua_from_far'], basezoning['max_dua_from_height']], axis=1).min(axis=1)
    basezoning['base_far'] = pd.concat([basezoning['max_far'], basezoning['max_far_from_height']], axis=1).min(axis=1)

    LOGGER.info(basezoning.head(3))

    #### put base zoning and upzoning together
    # merge base zoning and upzoning
    zoning_comp = pd.merge(
        upzoning_parcel,
        basezoning,
        on='PARCEL_ID',
        how='left'
    )
    LOGGER.info('merge base zoning and upzoning, total {} rows'.format(len(zoning_comp)))

    # add zone and SD info
    zoning_comp = pd.merge(
        zoning_comp,
        parcel_SD, on='PARCEL_ID', how='left'
    )
    LOGGER.info('mergeed with SD field, total {} rows'.format(len(zoning_comp)))

    # flag parcels with base DUA > upzoning
    zoning_comp['dua_comp'] = ''
    zoning_comp.loc[zoning_comp['dua_up'].isnull(), 'dua_comp'] = 'no upzoning'
    zoning_comp.loc[zoning_comp['base_dua'].isnull(), 'dua_comp'] = 'no base zoning'
    zoning_comp.loc[
        zoning_comp['base_dua'].notnull() & \
        zoning_comp['dua_up'].notnull() & \
        (zoning_comp['base_dua'] > zoning_comp['dua_up']),
        'dua_comp'] = 'base DUA > upzoning'
    zoning_comp.loc[
        zoning_comp['base_dua'].notnull() & \
        zoning_comp['dua_up'].notnull() & \
        (zoning_comp['base_dua'] == zoning_comp['dua_up']),
        'dua_comp'] = 'base DUA = upzoning'
    zoning_comp.loc[
        zoning_comp['base_dua'].notnull() & \
        zoning_comp['dua_up'].notnull() & \
        (zoning_comp['base_dua'] < zoning_comp['dua_up']),
        'dua_comp'] = 'base DUA < upzoning'

    LOGGER.info('results of dua comparison: \n{}'.format(zoning_comp['dua_comp'].value_counts(dropna=False)))

    # merge with spatial data
    zoning_comp_gdf = pd.merge(
        zoning_comp,
        p10_geo[['PARCEL_ID', 'geometry']],
        on='PARCEL_ID',
        how='left'
    )
    LOGGER.info('merged with parcel spatial data, total {} rows'.format(len(zoning_comp_gdf)))
    zoning_comp_gdf = gpd.GeoDataFrame(zoning_comp_gdf, geometry='geometry')

    #### write out by county or SD
    
    LOGGER.info('write out spatial data to {}'.format(QAQC_DIR))
    if my_args.export_geo == 'county':
        LOGGER.info('write out data by {}: {}'.format(my_args.export_geo, zoning_comp_gdf['county'].unique()))
        for i in zoning_comp_gdf['county'].unique():
            subset = zoning_comp_gdf.loc[zoning_comp_gdf['county'] == i]
            subset.to_file(
                os.path.join(QAQC_DIR, 'zoning_comp_{}_{}.geojson'.format(my_args.scenario, i)), 
                driver='GeoJSON')
    elif my_args.export_geo == 'superdistrict':
        LOGGER.info('write out data by {}'.format(my_args.export_geo))
        for i in zoning_comp_gdf['Superdistrict'].unique():
            subset = zoning_comp_gdf.loc[zoning_comp_gdf['Superdistrict'] == i]
            subset.to_file(
                os.path.join(QAQC_DIR, 'zoning_comp_{}_SD{}.geojson'.format(my_args.scenario, i)), 
                driver='GeoJSON')
        