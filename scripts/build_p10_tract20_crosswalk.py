# build crosswalk between UrbanSim p10 parcel and Census Tract 2020 geography, using "centroid within" method.

import geopandas as gpd
import pandas as pd
import numpy as np 
import fiona
import os, time, logging

today = time.strftime('%Y_%m_%d')
analysis_crs = "EPSG:26910"

def get_centroid(x, ensure_within=True):
    """
    Given a shapely Polygon, returns the centroid.
    If ensure_within is True, and centroid is not
    within the polygon, returns the representative_point
    """
    try:
        centroid = x.centroid
        if ensure_within and not x.intersects(centroid):
            centroid = x.representative_point()
    except: pass
    return centroid


# Input

TRACT20_FILE = r'M:\Data\Census\Geography\tl_2020_06_tract\tl_2020_06_tract_bayarea.shp'

BOX_DIR = r'C:\Users\{}\Box\Modeling and Surveys\Urban Modeling\Bay Area UrbanSim\p10 Datasets for PBA2050plus'.format(os.getenv('USERNAME'))
P10_DATASET_PATH = os.path.join(BOX_DIR, 'raw_data_to_build_parcels_geography', 'PBA2050 Plus Blueprint Data QAQC - 20231206', 'PBA2050 Plus Blueprint Data QAQC.gdb')

# Output
M_DIR = r'M:\Application\PBA50Plus_Data_Processing\crosswalks'
P10_TRACT2020_CROSSWALK_FILE = os.path.join(M_DIR, 'p10_tract20_crosswalk.csv')
LOG_FILE = os.path.join(M_DIR, 'p10_tract20_crosswalk_{}.log'.format(today))

if __name__ == '__main__':
    # set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(ch)
    # file handler
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh)

    # load p10 - use the topo_fix version for spatial operation
    logger.info('loading p10 geography data from {}'.format(P10_DATASET_PATH))
    p10_layers = fiona.listlayers(P10_DATASET_PATH)
    logger.info('the geodatabase contains the following layers: {}'.format(p10_layers))
    p10_topo_fix = gpd.read_file(P10_DATASET_PATH, driver='fileGDB', layer='p10_parcels_topo_fix')
    logger.info('loaded {} rows of data, {} unique parcel_id'.format(
        p10_topo_fix.shape[0], p10_topo_fix['parcel_id'].nunique()))
    logger.info('current crs: {}'.format(p10_topo_fix.crs))
    # make sure it is 'EPSG:26910'
    p10_topo_fix = p10_topo_fix.to_crs(analysis_crs)

    # get parcel centroids
    p10_centroid = p10_topo_fix.copy()
    logger.info('creating parcel centroids')
    p10_centroid['centroid'] = p10_centroid['geometry'].map(get_centroid)
    logger.debug('check parcels without centroid: \n{}'.format(
        p10_centroid.loc[p10_centroid['centroid'].isnull()]
    ))
    
    # create a gdf of parcel_id and centrod geometry
    p10_centroid_gdf = gpd.GeoDataFrame(p10_centroid[['parcel_id']], geometry=p10_centroid['centroid'], crs=analysis_crs)
    logger.info('created parcel centroids table: \n{}'.format(p10_centroid_gdf.head()))

    # load tract20 data
    logger.info('loading tract20 shape from {}'.format(TRACT20_FILE))
    tract2020_geo = gpd.read_file(TRACT20_FILE)
    logger.info('loaded {} rows, {} unique GEOID'.format(tract2020_geo.shape[0], tract2020_geo['GEOID'].nunique()))
    logger.debug(tract2020_geo.head(3))

    logger.info('current crs: {}'.format(tract2020_geo.crs))
    tract2020_geo_proj = tract2020_geo.to_crs(analysis_crs)
    logger.info('converted to crs: {}'.format(tract2020_geo_proj.crs))

    # spatial join
    logger.info('spatial join - parcel centroid within Tract20')
    p10_tract20 = gpd.sjoin(p10_centroid_gdf, tract2020_geo_proj, how="left", predicate="within")
    logger.info('join completed, {} rows, {} unique parcel_id'.format(
        p10_tract20.shape[0],
        p10_tract20['parcel_id'].nunique()
    ))
    
    # some parcels at the edge of the region didn't get a join
    no_join = p10_tract20.loc[p10_tract20['GEOID'].isnull()]
    logger.info('{} rows not assigned to a tract'.format(no_join.shape[0]))
    
    if no_join.shape[0] > 0:
        qaqc_file = os.path.join(M_DIR, 'QAQC', 'p10_tract20_noJoin_qaqc.shp') 
        logger.info('write out to {} for QAQC'.format(qaqc_file))
        no_join.to_file(qaqc_file)

        logger.info('join these parcels to the nearest tract instead')
        no_join_nearest = gpd.sjoin_nearest(no_join[['parcel_id', 'geometry']], tract2020_geo_proj)
        logger.info('finished join, {} rows, {} unique parcel_id'.format(
            no_join_nearest.shape[0], no_join_nearest['parcel_id'].nunique()
        ))

        logger.info('merge it back')
        jointed = p10_tract20.loc[p10_tract20['GEOID'].notnull()]
        p10_tract20 = pd.concat([jointed, no_join_nearest])
        logger.info('total {} rows, {} unique parcel_id'.format(
            p10_tract20.shape[0], p10_tract20['parcel_id'].nunique()
        ))

        logger.info('double check parcels not assigned to a tract: {}'.format(
            p10_tract20.loc[p10_tract20['GEOID'].isnull()].shape[0]))

    # join it back to the raw parcel and write out spatial file for QAQC
    p10_raw_shp = gpd.read_file(P10_DATASET_PATH, driver='fileGDB', layer='p10_parcels_raw')
    p10_tract20_shp = p10_raw_shp[['PARCEL_ID', 'geometry']].merge(
        p10_tract20[['parcel_id', 'STATEFP', 'COUNTYFP', 'TRACTCE', 'GEOID', 'NAME', 'NAMELSAD', 'MTFCC', 'FUNCSTAT']].rename(
            columns={'parcel_id': 'PARCEL_ID'}),
        on='PARCEL_ID',
        how='left'
    )
    all_parcel_qaqc_file = os.path.join(M_DIR, 'QAQC', 'p10_tract20.shp')
    logger.info('write out {} rows of QAQC spatial data to {}'.format(p10_tract20_shp.shape[0], all_parcel_qaqc_file))
    p10_tract20_shp.to_file(all_parcel_qaqc_file)

    # write out CSV file
    logger.info('write out crosswalk to {}'.format(P10_TRACT2020_CROSSWALK_FILE))
    p10_tract20_csv = p10_tract20_shp.loc[:, p10_tract20_shp.columns != 'geometry']
    p10_tract20_csv.to_csv(P10_TRACT2020_CROSSWALK_FILE, index=False)
    