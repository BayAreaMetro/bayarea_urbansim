import pandas as pd
import geopandas as gpd
import logging
from datetime import datetime
import pathlib
import getpass
import os

# make global so we only read once
rtp2025_geography_crosswalk_df  = pd.DataFrame() # parcel -> zoning categories (epc, displacement, growth geog, hra, tra, ppa), jurisdiction
rtp2025_tract_crosswalk_df      = pd.DataFrame() # parcel -> tract10 and tract20
rtp2025_urban_area_crosswalk_df = pd.DataFrame() # parcel -> in 2020 urbanized area footprint

rtp2025_transit_service_df      = pd.DataFrame() # parcel -> transit service
rtp2025_taz_crosswalk_df        = pd.DataFrame() # taz1 -> epc
rtp2025_parcel_taz_crosswalk_df = pd.DataFrame() # parcel -> taz1
parcel_taz_sd_crosswalk_df      = pd.DataFrame() # parcel -> taz1 and superdistrict

pba50_geography_crosswalk_df = pd.DataFrame() # parcel -> PBA50 growth geographies for use in rtp2025 metrics

rtp2025_np_parcel_inundation_df    = pd.DataFrame() # parcel -> parcel sea level rise inundation
rtp2025_dbp_parcel_inundation_df    = pd.DataFrame() # parcel -> parcel sea level rise inundation

rtp2021_tract_crosswalk_df      = pd.DataFrame() # parcel -> tracts, including coc/epc, displacement, growth geography, HRA, TRA, PPA
rtp2021_pda_crosswalk_df        = pd.DataFrame() # parcel -> PDA (pda_id_pba50_fb)
rtp2021_geography_crosswalk_df  = pd.DataFrame() # parcel -> parcel category (fbpchcat -> growth geog, hra, tra), jurisdiction

rtp2021_np_parcel_inundation_df    = pd.DataFrame() # parcel -> parcel sea level rise inundation
rtp2021_fbp_parcel_inundation_df    = pd.DataFrame() # parcel -> parcel sea level rise inundation

PARCEL_AREA_FILTERS = {
    'RTP2021': {
            'HRA'       : lambda df: df['hra_id'] == 'HRA',
            'TRA'       : lambda df: df['tra_id'] != 'NA',  # note this is the string NA
            'HRAandTRA' : lambda df: (df['tra_id'] != 'NA') & (df['hra_id'] == 'HRA'),
            'GG'        : lambda df: df['gg_id'] == 'GG',
            'nonGG'     : lambda df: df['gg_id'] != 'GG',
            'GG_nonPDA' : lambda df: (df['gg_id'] == 'GG') & (pd.isna(df['pda_id_pba50_fb'])),
            'PDA'       : lambda df: pd.notna(df['pda_id_pba50_fb']),
            'EPC'       : lambda df: df['tract10_epc'] == 1,
            'nonEPC'    : lambda df: df['tract10_epc'] != 1,
            'PPA'       : lambda df: df['ppa_id'] == 'ppa',
            'Region'    : None
    },
    'RTP2025': {
            'HRA'       : lambda df: df['hra_id'] == 'HRA',
            'TRA'       : lambda df: df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']),
            'HRAandTRA' : lambda df: (df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])) & (df['hra_id'] == 'HRA'),
            'GG'        : lambda df: df['gg_id'] == 'GG',
            'nonGG'     : lambda df: df['gg_id'] != 'GG',
            'PBA50GG'   : lambda df: df['pba50_gg_id'] == 'GG',
            'PBA50nonGG': lambda df: df['pba50_gg_id'] != 'GG',
            'GG_nonPDA' : lambda df: (df['gg_id'] == 'GG') & (pd.isna(df['pda_id'])),
            'PDA'       : lambda df: pd.notna(df['pda_id']),
            'EPC'       : lambda df: df['epc_id'] == 'EPC',
            'nonEPC'    : lambda df: df['epc_id'] != 'EPC',
            'PPA'       : lambda df: df['ppa_id'] == 'PPA',
            'Region'    : None
    }
}

# set the path for M: drive
# from OSX, M:/ may be mounted to /Volumes/Data/Models
M_DRIVE = pathlib.Path("/Volumes/Data/Models") if os.name != "nt" else pathlib.Path("M:/")
USERNAME = getpass.getuser()
HOME_DIR = pathlib.Path.home()
if USERNAME.lower() in ['lzorn']:
    BOX_DIR = pathlib.Path("E:/Box")
else:
    BOX_DIR = HOME_DIR / 'Box'

# --------------------------------------
# Data Loading Based on Model Run Plan
# --------------------------------------
def load_data_for_runs(
        rtp: str,
        METRICS_DIR: pathlib.Path,
        run_directory_path: pathlib.Path,
        modelrun_alias: str,
        no_interpolate: bool = False,
        skip_base_year: bool = False
    ):
    """
    Reads crosswalk data as well as parcel data and county summary data for the given BAUS model run
    for both the base year and the horizon year (which varies based on the rtp).

    Parameters:
    - rtp (str): one of RTP2021 or RTP2025
    - METRICS_DIR (pathlib.Path): metrics directory for finding crosswalks
    - run_directory_path (pathlib.Path): path for model run output files
    - modelrun_alias (str): alias for the model run. e.g. 'No Project', 'DBP, etc.
    - no_interpolate (bool): if True, don't read 2025 data and interpolate to 2023.
      No effect for RTP2021.
    - skip_base_year (bool): whether to skip reading 2020/2025 data because we're going
      to reuse previously ingested No Project base year data

    Returns:
    - dict with year -> {
        "parcel" -> parcel DataFrame, 
        "county" -> county DataFrame,
        "TAZ1454"-> taz DataFrame (necessary for totpop, which is only tabulated for TAZs)
      }
    
    """
    # make global so we only read once
    global rtp2025_geography_crosswalk_df
    global rtp2025_tract_crosswalk_df
    global rtp2025_urban_area_crosswalk_df
    global rtp2025_transit_service_df
    global rtp2025_taz_crosswalk_df

    global rtp2025_parcel_taz_crosswalk_df
    global parcel_taz_sd_crosswalk_df
    global rtp2025_np_parcel_inundation_df
    global rtp2025_dbp_parcel_inundation_df
    global pba50_geography_crosswalk_df

    global rtp2021_geography_crosswalk_df
    global rtp2021_tract_crosswalk_df
    global rtp2021_pda_crosswalk_df
    global rtp2021_np_parcel_inundation_df
    global rtp2021_fbp_parcel_inundation_df

    CROSSWALKS_DIR = M_DRIVE / "urban_modeling" / "baus" / "BAUS Inputs" / "basis_inputs" / "crosswalks"

    # Start by pre-canning a parcel_id to zone_id, county, and superdistrict crosswalk DF
    # This crosswalk is the same for RTP2021 and RTP2025
    if len(parcel_taz_sd_crosswalk_df) == 0:
        bayareafips = {
            "06001": "Alameda",
            "06013": "Contra Costa",
            "06041": "Marin",
            "06055": "Napa",
            "06075": "San Francisco",
            "06081": "San Mateo",
            "06085": "Santa Clara",
            "06097": "Sonoma",
            "06095": "Solano",
        }

        PARCEL_TAZ_CROSSWALK_FILE = CROSSWALKS_DIR / "2020_08_17_parcel_to_taz1454sub.csv"
        parcel_taz_crosswalk_df = pd.read_csv(PARCEL_TAZ_CROSSWALK_FILE, usecols=['PARCEL_ID', 'ZONE_ID', 'manual_county'])
        parcel_taz_crosswalk_df.columns = parcel_taz_crosswalk_df.columns.str.lower()
        parcel_taz_crosswalk_df["county"] = parcel_taz_crosswalk_df['manual_county'].map(
            lambda x: f"06{x:03d}"
        ).map(bayareafips)
        del parcel_taz_crosswalk_df['manual_county']
        logging.info(f"  Read {len(parcel_taz_crosswalk_df):,} rows from crosswalk {PARCEL_TAZ_CROSSWALK_FILE}")
        logging.debug(f"  parcel_taz_crosswalk_df.head():\n{parcel_taz_crosswalk_df.head()}")

        TAZ_SD_CROSSWALK_FILE = CROSSWALKS_DIR / "taz_geography.csv"
        taz_sd_crosswalk_df = pd.read_csv(TAZ_SD_CROSSWALK_FILE, usecols=['zone', 'superdistrict'])
        taz_sd_crosswalk_df.rename(columns={"zone": "zone_id"}, inplace=True)
        logging.info(f"  Read {len(taz_sd_crosswalk_df):,} rows from crosswalk {TAZ_SD_CROSSWALK_FILE}")
        logging.debug(f"  taz_sd_crosswalk_df.head():\n{taz_sd_crosswalk_df.head()}")

        parcel_taz_sd_crosswalk_df = pd.merge(
            left     = parcel_taz_crosswalk_df,
            right    = taz_sd_crosswalk_df,
            on       = 'zone_id',
            how      = 'left',
            validate = 'many_to_one'
        )


    # year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    modelrun_data = {}
    if rtp == "RTP2025":
        if len(rtp2025_geography_crosswalk_df) == 0:
            PARCEL_CROSSWALK_FILE = CROSSWALKS_DIR / "parcels_geography_2024_02_14.csv"
            rtp2025_geography_crosswalk_df = pd.read_csv(PARCEL_CROSSWALK_FILE, usecols=['PARCEL_ID','ACRES','dis_id','tra_id','gg_id','pda_id','hra_id','epc_id','ppa_id','ugb_id','juris'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_geography_crosswalk_df), PARCEL_CROSSWALK_FILE))
            logging.debug("  rtp2025_geography_crosswalk_df.head():\n{}".format(rtp2025_geography_crosswalk_df.head()))
            logging.debug(f"  rtp2025_geography_crosswalk_df['ppa_id'].value_counts(dropna=False)=\n{rtp2025_geography_crosswalk_df['ppa_id'].value_counts(dropna=False)}")
            logging.debug(f"  {len(rtp2025_geography_crosswalk_df.loc[pd.isna(rtp2025_geography_crosswalk_df.ppa_id)])=}")
            logging.debug(f"  rtp2025_geography_crosswalk_df['gg_id'].value_counts(dropna=False)=\n{rtp2025_geography_crosswalk_df['gg_id'].value_counts(dropna=False)}")

            # jurisdiction: standardize to Title Case, with spaces
            rtp2025_geography_crosswalk_df.rename(columns={'juris':'jurisdiction'}, inplace=True)
            rtp2025_geography_crosswalk_df['jurisdiction'] = rtp2025_geography_crosswalk_df.jurisdiction.str.replace("_"," ")
            rtp2025_geography_crosswalk_df['jurisdiction'] = rtp2025_geography_crosswalk_df.jurisdiction.str.title()
            rtp2025_geography_crosswalk_df['jurisdiction'] = rtp2025_geography_crosswalk_df.jurisdiction.str.replace("St ","St. ") # St. Helena
            logging.debug(f"rtp2025_geography_crosswalk_df.jurisdiction.value_counts(dropna=False):\n{rtp2025_geography_crosswalk_df.jurisdiction.value_counts(dropna=False)}")
        
        if len(pba50_geography_crosswalk_df) == 0:
            PBA50_PARCEL_CROSSWALK_FILE = CROSSWALKS_DIR / "2021_02_25_parcels_geography.csv"
            pba50_geography_crosswalk_df = pd.read_csv(PBA50_PARCEL_CROSSWALK_FILE, usecols=['PARCEL_ID','gg_id'])
            pba50_geography_crosswalk_df.rename(columns={"gg_id":"pba50_gg_id"}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(pba50_geography_crosswalk_df), PBA50_PARCEL_CROSSWALK_FILE))
            logging.debug("  pba50_geography_crosswalk_df.head():\n{}".format(pba50_geography_crosswalk_df.head()))

        if len(rtp2025_urban_area_crosswalk_df) == 0:
            URBAN_AREA_CROSSWALK_FILE = CROSSWALKS_DIR / "p10_parcels_to_2020_urban_areas.csv"
            rtp2025_urban_area_crosswalk_df = pd.read_csv(URBAN_AREA_CROSSWALK_FILE, usecols=['parcel_id', 'in_urban_area'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_urban_area_crosswalk_df), URBAN_AREA_CROSSWALK_FILE))
            logging.debug("  rtp2025_urban_area_crosswalk_df.head():\n{}".format(rtp2025_urban_area_crosswalk_df.head()))

        # transit service areas (used through April 2024 - with n-category transit service areas including headway differentiation)
        # We used with transit_service_area_share_v2().

        # if len(rtp2025_transit_service_df) == 0:
        #     import geopandas as gpd
        #     PARCEL_TRANSITSERVICE_FILE = M_DRIVE / "Data" / "GIS layers" / "JobsHousingTransitProximity" / "update_2024" / "outputs" / "p10_topofix_classified.parquet"
        #     rtp2025_transit_service_df = pd.read_parquet(PARCEL_TRANSITSERVICE_FILE)
        #     transit_cols_keep = ['PARCEL_ID','area_type','Service_Level_np_cat5', 'Service_Level_fbp_cat5', 'Service_Level_current_cat5']
        #     rtp2025_transit_service_df = rtp2025_transit_service_df[transit_cols_keep]
        #     logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_transit_service_df), PARCEL_TRANSITSERVICE_FILE))
        #     logging.debug("  rtp2025_transit_service_df.head():\n{}".format(rtp2025_transit_service_df.head()))

        # simpler version with binary 1/0 classification instead of headway differentiation. We use with transit_service_area_share_v2().
        
        if len(rtp2025_transit_service_df) == 0:
            import geopandas as gpd
            PARCEL_TRANSITSERVICE_FILE = BOX_DIR / 'Plan Bay Area 2050+' / 'Blueprint' / \
                'Draft Blueprint Modeling and Metrics' / \
                'transportation' / "p10_x_transit_area_identity.csv"
            rtp2025_transit_service_df = pd.read_csv(PARCEL_TRANSITSERVICE_FILE, usecols=['parcel_id','cur','np', 'dbp'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_transit_service_df), PARCEL_TRANSITSERVICE_FILE))
            logging.debug("  rtp2025_transit_service_df.head():\n{}".format(rtp2025_transit_service_df.head()))

        # tract
        if len(rtp2025_tract_crosswalk_df) == 0:
            # map to census 2010 tract and census 2020 tract
            TRACT_CROSSWALK_FILE = CROSSWALKS_DIR / "p10_census.csv"
            rtp2025_tract_crosswalk_df = pd.read_csv(TRACT_CROSSWALK_FILE, usecols=['parcel_id','tract10','tract20'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_tract_crosswalk_df), TRACT_CROSSWALK_FILE))
            logging.info("  len(rtp2025_tract_crosswalk_df.tract10.unique()): {:,}  parcels with null tract10: {:,}".format(
                len(rtp2025_tract_crosswalk_df.tract10.unique()),
                len(rtp2025_tract_crosswalk_df.loc[ pd.isnull(rtp2025_tract_crosswalk_df.tract10) ])))
            logging.info("  len(rtp2025_tract_crosswalk_df.tract20.unique()): {:,}  parcels with null tract20: {:,}".format(
                len(rtp2025_tract_crosswalk_df.tract20.unique()),
                len(rtp2025_tract_crosswalk_df.loc[ pd.isnull(rtp2025_tract_crosswalk_df.tract20) ])))
            # set nulls to -1 so type is int64. Note: smaller ints are too small
            rtp2025_tract_crosswalk_df.fillna(-1, inplace=True)
            rtp2025_tract_crosswalk_df = rtp2025_tract_crosswalk_df.astype('int64')
            logging.debug("  rtp2025_tract_crosswalk_df.head():\n{}".format(rtp2025_tract_crosswalk_df.head()))

            # tract-based lookups
            TRACT_EPC_CROSSWALK_FILE = "https://raw.githubusercontent.com/BayAreaMetro/Spatial-Analysis-Mapping-Projects/master/Project-Documentation/Equity-Priority-Communities/Data/epc_acs2022.csv"
            tract_epc_df = pd.read_csv(TRACT_EPC_CROSSWALK_FILE, usecols=['tract_geoid','epc_2050p'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_epc_df), TRACT_EPC_CROSSWALK_FILE))
            # EPCs are defined with tract20 -> rename
            tract_epc_df.rename(columns = {"tract_geoid":"tract20", "epc_2050p":"tract20_epc"}, inplace=True)
            logging.info("  len(tract_epc_df.tract20.unique()): {:,}".format(len(tract_epc_df.tract20.unique())))
            logging.debug("  tract_epc_df.head():\n{}".format(tract_epc_df.head()))
            rtp2025_tract_crosswalk_df = pd.merge(
                left     = rtp2025_tract_crosswalk_df,
                right    = tract_epc_df,
                on       = 'tract20',
                how      = 'left',
                validate = 'many_to_one'
            )

            TRACT_GG_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "tract20_ggPBA50plus_2024_02_29.csv"
            tract_gg_df = pd.read_csv(TRACT_GG_CROSSWALK_FILE, usecols=['GEOID', 'growth_geo'])
            tract_gg_df.rename(columns={'GEOID':'tract20', 'growth_geo':'tract20_growth_geo'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_gg_df), TRACT_GG_CROSSWALK_FILE))
            logging.debug("  tract_gg_df.head():\n{}".format(tract_gg_df.head()))
            rtp2025_tract_crosswalk_df = pd.merge(
                left     = rtp2025_tract_crosswalk_df,
                right    = tract_gg_df,
                on       = 'tract20',
                how      = 'left',
                validate = 'many_to_one'
            )

            TRACT_TRA_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "tract20_ggtraPBA50plus_2024_02_29.csv"
            tract_tra_df = pd.read_csv(TRACT_TRA_CROSSWALK_FILE, usecols=['GEOID', 'gg_tra']) 
            tract_tra_df.rename(columns={'GEOID':'tract20', 'gg_tra':'tract20_tra'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_tra_df), TRACT_TRA_CROSSWALK_FILE))
            logging.debug("  tract_tra_df.head():\n{}".format(tract_tra_df.head()))
            rtp2025_tract_crosswalk_df = pd.merge(
                left     = rtp2025_tract_crosswalk_df,
                right    = tract_tra_df,
                on       = 'tract20',
                how      = 'left',
                validate = 'many_to_one'
            )

            TRACT_HRA_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "tract20_hraPBA50plus_2024_02_29.csv"
            tract_hra_df = pd.read_csv(TRACT_HRA_CROSSWALK_FILE, usecols=['GEOID','taz_hra'])  # odd that it's called taz_hra
            tract_hra_df.rename(columns={'GEOID':'tract20', 'taz_hra':'tract20_hra'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_hra_df), TRACT_HRA_CROSSWALK_FILE))
            logging.debug("  tract_hra_df.head():\n{}".format(tract_hra_df.head()))
            rtp2025_tract_crosswalk_df = pd.merge(
                left     = rtp2025_tract_crosswalk_df,
                right    = tract_hra_df,
                on       = 'tract20',
                how      = 'left',
                validate = 'many_to_one'
            )

            # displacement risk - udp_file/udp_DR_df
            # NOTE: these are 2010 tracts but we need them to be 2020 tracts
            TRACT_DISPLACEMENT_FILE = METRICS_DIR / "metrics_input_files" / "udp_2017results.csv"
            tract_displacement_df = pd.read_csv(TRACT_DISPLACEMENT_FILE, usecols=['Tract','DispRisk'])
            tract_displacement_df.rename(columns={'Tract':'tract10', 'DispRisk':'tract10_DispRisk'}, inplace=True)
            # tract10 doesn't have state code; add it
            tract_displacement_df['tract10'] = tract_displacement_df['tract10'].astype('int64')
            tract_displacement_df.tract10 += 6000000000
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_displacement_df), TRACT_DISPLACEMENT_FILE))
            logging.debug("  tract_displacement_df.head():\n{}".format(tract_displacement_df.head()))

            rtp2025_tract_crosswalk_df = pd.merge(
                left     = rtp2025_tract_crosswalk_df,
                right    = tract_displacement_df,
                on       = 'tract10',
                how      = 'left',
                validate = 'many_to_one',
                indicator=True
            )
            logging.debug("rtp2025_tract_crosswalk_df._merge.value_counts():\n{}".format(
                          rtp2025_tract_crosswalk_df._merge.value_counts()))
            rtp2025_tract_crosswalk_df.drop(columns=['_merge'], inplace=True)

            # fillna with zero
            rtp2025_tract_crosswalk_df.fillna(0, inplace=True)

            logging.debug("final rtp2025_tract_crosswalk_df.head():\n{}".format(rtp2025_tract_crosswalk_df))
            logging.debug("final rtp2025_tract_crosswalk_df.dtypes():\n{}".format(rtp2025_tract_crosswalk_df.dtypes))
            # columns are: parcel_id, tract10, tract20, tract20_epc, tract20_growth_geo, tract20_tra, tract20_hra, tract10_DispRisk


        if len(rtp2025_taz_crosswalk_df) == 0:

            # taz-based lookups
            TAZ_EPC_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "taz1454_epcPBA50plus_2024_02_29.csv"
            rtp2025_taz_crosswalk_df = pd.read_csv(TAZ_EPC_CROSSWALK_FILE, usecols=['TAZ1454','taz_epc'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_taz_crosswalk_df), TAZ_EPC_CROSSWALK_FILE))
            logging.debug("  rtp2025_taz_crosswalk_df.head():\n{}".format(rtp2025_taz_crosswalk_df.head()))

        if len(rtp2025_parcel_taz_crosswalk_df)==0:

            # parcels to taz crosswalk - we need this for the area_type (suburban/urban/rural) taz-based classification

            # Reuse our earlier, RTP-agnostic parcel-to-TAZ-and-SD crosswalk
            rtp2025_parcel_taz_crosswalk_df = parcel_taz_sd_crosswalk_df.copy()
            logging.debug("  rtp2025_parcel_taz_crosswalk_df.head():\n{}".format(rtp2025_parcel_taz_crosswalk_df.head()))
            
            # taz-based lookups to area_type (urban/suburban/rural)
            TAZ_AREATYPE_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "taz_urban_suburban.csv"
            rtp2025_taz_areatype_crosswalk_df = pd.read_csv(TAZ_AREATYPE_CROSSWALK_FILE, usecols=['TAZ1454','area_type'])
            logging.info("  Read {:,} rows from taz areatype crosswalk {}".format(len(rtp2025_taz_areatype_crosswalk_df), TAZ_AREATYPE_CROSSWALK_FILE))
            logging.debug("  rtp2025_taz_areatype_crosswalk_df.head():\n{}".format(rtp2025_taz_areatype_crosswalk_df.head()))

           
            rtp2025_parcel_taz_crosswalk_df = pd.merge(
                left     = rtp2025_parcel_taz_crosswalk_df,
                right    = rtp2025_taz_areatype_crosswalk_df,
                left_on  = 'zone_id',
                right_on = 'TAZ1454',
                how      = 'left',
                validate = 'many_to_one',
                indicator=True
            )
            logging.debug("rtp2025_parcel_taz_crosswalk_df._merge.value_counts():\n{}".format(
                          rtp2025_parcel_taz_crosswalk_df._merge.value_counts()))
            rtp2025_parcel_taz_crosswalk_df.drop(columns=['_merge'], inplace=True)

            # fillna with zero
            rtp2025_parcel_taz_crosswalk_df.fillna(0, inplace=True)

            logging.debug("rtp2025_parcel_taz_crosswalk_df.head():\n{}".format(rtp2025_parcel_taz_crosswalk_df))
            logging.debug("rtp2025_parcel_taz_crosswalk_df.dtypes():\n{}".format(rtp2025_parcel_taz_crosswalk_df.dtypes))
            
            
        if len(rtp2025_np_parcel_inundation_df) == 0:
            PARCEL_INUNDATION_FILE = METRICS_DIR / "metrics_input_files" / "slr_parcel_inundation_PBA50Plus_NP.csv"
            rtp2025_np_parcel_inundation_df = pd.read_csv(PARCEL_INUNDATION_FILE)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_np_parcel_inundation_df), PARCEL_INUNDATION_FILE))
            logging.debug("  rtp2025_np_parcel_inundation_df.head():\n{}".format(rtp2025_np_parcel_inundation_df.head()))

        if len(rtp2025_dbp_parcel_inundation_df) == 0:
            PARCEL_INUNDATION_FILE = METRICS_DIR / "metrics_input_files" / "slr_parcel_inundation_PBA50Plus_DBP.csv"
            rtp2025_dbp_parcel_inundation_df = pd.read_csv(PARCEL_INUNDATION_FILE)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_dbp_parcel_inundation_df), PARCEL_INUNDATION_FILE))
            logging.debug("  rtp2025_dbp_parcel_inundation_df.head():\n{}".format(rtp2025_dbp_parcel_inundation_df.head()))

        # define analysis years
        if skip_base_year:
            logging.info(f"Skipping 2020 {'' if no_interpolate else 'and 2025 '}data because we're reusing the No Project base year data")
        else:
            modelrun_data[2020] = {}
            if not no_interpolate:
                modelrun_data[2025] = {}  # for later interpolation to 2023
        modelrun_data[2050]  = {}
        parcel_pattern       = "core_summaries/*_parcel_summary_{}.csv"
        geo_summary_pattern  = "geographic_summaries/*_county_summary_{}.csv"
        taz1_summary_pattern = "travel_model_summaries/*_taz1_summary_{}.csv"
        taz1_interim_summary_pattern = "core_summaries/*_interim_zone_output_{}.csv"

    elif rtp == "RTP2021":
        # these are all tract-based -- load into one dataframe
        if len(rtp2021_tract_crosswalk_df) == 0:

            # pba50_metrics.py called this parcel_tract_crosswalk_file/parcel_tract_crosswalk_df
            TRACT_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "parcel_tract_crosswalk.csv"
            rtp2021_tract_crosswalk_df = pd.read_csv(TRACT_CROSSWALK_FILE, usecols=['parcel_id','tract_id'])
            rtp2021_tract_crosswalk_df.rename(columns={'tract_id':'tract10'}, inplace=True) # rename for clarity
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2021_tract_crosswalk_df), TRACT_CROSSWALK_FILE))
            logging.info("  rtp2021_tract_crosswalk_df.tract10.unique count:{:,}".format(
                len(rtp2021_tract_crosswalk_df.tract10.unique())
            ))
            logging.debug("  rtp2021_tract_crosswalk_df.head():\n{}".format(rtp2021_tract_crosswalk_df.head()))

            # pba50_metrics.py called this coc_flag_file/coc_flag_df
            COC_FLAG_FILE = METRICS_DIR / "metrics_input_files" / "COCs_ACS2018_tbl_TEMP.csv"
            tract_coc_df = pd.read_csv(COC_FLAG_FILE, usecols=['tract_id','coc_flag_pba2050'])
            tract_coc_df.rename(columns={'tract_id':'tract10', 'coc_flag_pba2050':'tract10_epc'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_coc_df), COC_FLAG_FILE))
            logging.debug("  tract_coc_df.head():\n{}".format(tract_coc_df.head()))
            # merge it into rtp2021_tract_crosswalk_df
            rtp2021_tract_crosswalk_df = pd.merge(
                left     = rtp2021_tract_crosswalk_df,
                right    = tract_coc_df,
                on       = 'tract10',
                how      = 'left',
                validate = 'many_to_one',
                indicator= True
            )
            logging.debug("rtp2021_tract_crosswalk_df merged with COC: {:,} rows, _merge=\n{}".format(
                len(rtp2021_tract_crosswalk_df), rtp2021_tract_crosswalk_df._merge.value_counts()))
            rtp2021_tract_crosswalk_df.drop(columns=['_merge'], inplace=True)

            # displacement risk - udp_file/udp_DR_df
            TRACT_DISPLACEMENT_FILE = METRICS_DIR / "metrics_input_files" / "udp_2017results.csv"
            tract_displacement_df = pd.read_csv(TRACT_DISPLACEMENT_FILE, usecols=['Tract','DispRisk'])
            tract_displacement_df.rename(columns={'Tract':'tract10', 'DispRisk':'tract10_DispRisk'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_displacement_df), TRACT_DISPLACEMENT_FILE))
            logging.debug("  tract_displacement_df.head():\n{}".format(tract_displacement_df.head()))
            # merge it into rtp2021_tract_crosswalk_df
            rtp2021_tract_crosswalk_df = pd.merge(
                left     = rtp2021_tract_crosswalk_df,
                right    = tract_displacement_df,
                on       = 'tract10',
                how      = 'left',
                validate = 'many_to_one',
                indicator= True
            )
            logging.debug("rtp2021_tract_crosswalk_df merged with DispRisk: {:,} rows, _merge=\n{}".format(
                len(rtp2021_tract_crosswalk_df), rtp2021_tract_crosswalk_df._merge.value_counts()))
            rtp2021_tract_crosswalk_df.drop(columns=['_merge'], inplace=True)

            # tract_HRA_xwalk_file/tract_HRA_xwalk_df
            TRACT_HRA_FILE = METRICS_DIR / "metrics_input_files" / "tract_hra_xwalk.csv"
            tract_hra_df = pd.read_csv(TRACT_HRA_FILE, usecols=['tract_id','hra','tra','growth_geo'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_hra_df), TRACT_HRA_FILE))
            # rename hra, tra, growth_geo to make it clear these are tract-baed
            tract_hra_df.rename(columns={'tract_id':'tract10', 'hra':'tract10_hra', 'tra':'tract10_tra', 
                                         'growth_geo':'tract10_growth_geo'}, inplace=True)

            logging.debug("  tract_hra_df.head():\n{}".format(tract_hra_df.head()))
            # merge it into rtp2021_tract_crosswalk_df
            rtp2021_tract_crosswalk_df = pd.merge(
                left     = rtp2021_tract_crosswalk_df,
                right    = tract_hra_df,
                on       = 'tract10',
                how      = 'left',
                validate = 'many_to_one',
                indicator= True
            )
            logging.debug("rtp2021_tract_crosswalk_df merged with HRA xwalk: {:,} rows, _merge=\n{}".format(
                len(rtp2021_tract_crosswalk_df), rtp2021_tract_crosswalk_df._merge.value_counts()))
            rtp2021_tract_crosswalk_df.drop(columns=['_merge'], inplace=True)

            # fillna with zero and make int
            rtp2021_tract_crosswalk_df.fillna(0, inplace=True)
            rtp2021_tract_crosswalk_df = rtp2021_tract_crosswalk_df.astype(int)

            logging.debug("final rtp2021_tract_crosswalk_df.head():\n{}".format(rtp2021_tract_crosswalk_df))
            # columns are: parcel_id, tract10, tract10_epc, tract10_DispRisk, tract10_hra, tract10_growth_geo, tract10_tra

        # transit service areas (used through April 2024 - with n-category transit service areas including headway differentiation)
        # We used with transit_service_area_share_v2().

        # if len(rtp2025_transit_service_df) == 0:
        #     import geopandas as gpd
        #     PARCEL_TRANSITSERVICE_FILE = M_DRIVE / "Data" / "GIS layers" / "JobsHousingTransitProximity" / "update_2024" / "outputs" / "p10_topofix_classified.parquet"
        #     rtp2025_transit_service_df = pd.read_parquet(PARCEL_TRANSITSERVICE_FILE)
        #     transit_cols_keep = ['PARCEL_ID','area_type','Service_Level_np_cat5', 'Service_Level_fbp_cat5', 'Service_Level_current_cat5']
        #     rtp2025_transit_service_df = rtp2025_transit_service_df[transit_cols_keep]
        #     logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_transit_service_df), PARCEL_TRANSITSERVICE_FILE))
        #     logging.debug("  rtp2025_transit_service_df.head():\n{}".format(rtp2025_transit_service_df.head()))

        # simpler version with binary 1/0 classification instead of headway differentiation. We use with transit_service_area_share_v2().
        
        if len(rtp2025_transit_service_df) == 0:
            import geopandas as gpd
            PARCEL_TRANSITSERVICE_FILE = BOX_DIR / 'Plan Bay Area 2050+' / 'Blueprint' / \
                'Draft Blueprint Modeling and Metrics' / \
                'transportation' / "p10_x_transit_area_identity.csv"
            rtp2025_transit_service_df = pd.read_csv(PARCEL_TRANSITSERVICE_FILE, usecols=['parcel_id','cur','np', 'dbp'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_transit_service_df), PARCEL_TRANSITSERVICE_FILE))
            logging.debug("  rtp2025_transit_service_df.head():\n{}".format(rtp2025_transit_service_df.head()))

        if len(rtp2021_pda_crosswalk_df) == 0:
            # pba50_metrics.py called this parcel_GG_newxwalk_file/parcel_GG_newxwalk_df
            PDA_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "parcel_tra_hra_pda_fbp_20210816.csv"
            rtp2021_pda_crosswalk_df = pd.read_csv(PDA_CROSSWALK_FILE, usecols=['PARCEL_ID','pda_id_pba50_fb'])
            rtp2021_pda_crosswalk_df.rename(columns={'PARCEL_ID':'parcel_id'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2021_pda_crosswalk_df), PDA_CROSSWALK_FILE))
            logging.debug("  rtp2021_pda_crosswalk_df.head():\n{}".format(rtp2021_pda_crosswalk_df.head()))

        if len(rtp2021_geography_crosswalk_df) == 0:
            # pba50_metrics.py called this "parcel_geography_file" - use it to get fbpchcat
            GEOGRAPHY_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "2021_02_25_parcels_geography.csv"
            rtp2021_geography_crosswalk_df = pd.read_csv(GEOGRAPHY_CROSSWALK_FILE, usecols=['PARCEL_ID','fbpchcat','ppa_id','eir_coc_id', 'juris_name_full'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2021_geography_crosswalk_df), GEOGRAPHY_CROSSWALK_FILE))
            logging.debug("  rtp2021_geography_crosswalk_df.head():\n{}".format(rtp2021_geography_crosswalk_df.head()))

            # Expand fbpchcat into component parts
            parcel_zoning_df = rtp2021_geography_crosswalk_df['fbpchcat'].str.extract(
                r'^(?P<gg_id>GG|NA)(?P<tra_id>tra1|tra2c|tra2b|tra2a|tra2|tra3a|tra3|NA)(?P<hra_id>HRA)?(?P<dis_id>DIS)?(?P<zone_remainder>.*)$')
            parcel_zoning_df['fbpchcat'] = rtp2021_geography_crosswalk_df['fbpchcat']
            logging.debug("parcel_zoning_df=\n{}".format(parcel_zoning_df.head(30)))

            # check if any are missed: if zone_remainder contains 'HRA' or 'DIS'
            zone_re_problem_df = parcel_zoning_df.loc[parcel_zoning_df.zone_remainder.str.contains("HRA|DIS", na=False, regex=True)]
            logging.debug("zone_re_problem_df nrows={} dataframe:\n{}".format(len(zone_re_problem_df), zone_re_problem_df))

            # join it back to rtp2021_geography_crosswalk_df
            rtp2021_geography_crosswalk_df = pd.concat([rtp2021_geography_crosswalk_df,
                                                        parcel_zoning_df.drop(columns=['fbpchcat'])], axis='columns')
            logging.debug("  rtp2021_geography_crosswalk_df.head() after fbpchcat split:\n{}".format(
                rtp2021_geography_crosswalk_df.head()))
            
            # jurisdiction: standardize to Title Case, with spaces
            rtp2021_geography_crosswalk_df.rename(columns={'juris_name_full':'jurisdiction'}, inplace=True)
            rtp2021_geography_crosswalk_df['jurisdiction'] = rtp2021_geography_crosswalk_df.jurisdiction.str.replace("_"," ")
            rtp2021_geography_crosswalk_df['jurisdiction'] = rtp2021_geography_crosswalk_df.jurisdiction.str.title()
            rtp2021_geography_crosswalk_df['jurisdiction'] = rtp2021_geography_crosswalk_df.jurisdiction.str.replace("St ","St. ") # St. Helena
            logging.debug(f"rtp2021_geography_crosswalk_df.jurisdiction.value_counts(dropna=False):\n{rtp2021_geography_crosswalk_df.jurisdiction.value_counts(dropna=False)}")

        if len(rtp2021_np_parcel_inundation_df) == 0:
            PARCEL_INUNDATION_FILE = METRICS_DIR / "metrics_input_files" / "slr_parcel_inundation_PBA50_NP.csv"
            rtp2021_np_parcel_inundation_df = pd.read_csv(PARCEL_INUNDATION_FILE)
            logging.info("  Read {:,} rows from file {}".format(len(rtp2021_np_parcel_inundation_df), PARCEL_INUNDATION_FILE))
            logging.debug("  rtp2021_np_parcel_inundation_df.head():\n{}".format(rtp2021_np_parcel_inundation_df.head()))

        if len(rtp2021_fbp_parcel_inundation_df) == 0:
            PARCEL_INUNDATION_FILE = METRICS_DIR / "metrics_input_files" / "slr_parcel_inundation_PBA50_FBP.csv"
            rtp2021_fbp_parcel_inundation_df = pd.read_csv(PARCEL_INUNDATION_FILE)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2021_fbp_parcel_inundation_df), PARCEL_INUNDATION_FILE))
            logging.debug("  rtp2021_fbp_parcel_inundation_df.head():\n{}".format(rtp2021_fbp_parcel_inundation_df.head()))

        # define analysis years
        modelrun_data[2015] = {}
        modelrun_data[2050] = {}
        parcel_pattern       = "*_parcel_data_{}.csv"
        geo_summary_pattern  = "*_county_summaries_{}.csv"
        taz1_summary_pattern = "*_taz_summaries_{}.csv"

    else:
        raise ValueError(f"Unrecognized plan: {rtp}")

    # Load parcels summaries
    for year in sorted(modelrun_data.keys()):
        # handle RTP2021 hacks
        if (rtp=="RTP2021") and (year == 2050) and (modelrun_alias=="No Project"):
            # comment was: this is for no project (which does not have UBI) but had some post processing Affordable housing added
            modified_parcel_pattern = parcel_pattern.replace(".csv", "_add_AH.csv")
        elif (rtp=="RTP2021") and (year == 2050) and (modelrun_alias in ['EIR Alt 1','EIR Alt 2']):
            modified_parcel_pattern = parcel_pattern.replace(".csv", "_no_UBI.csv")
        else:
            modified_parcel_pattern = parcel_pattern

        logging.debug("Looking for parcel data matching {}".format(modified_parcel_pattern.format(year)))
        file = next(run_directory_path.glob(modified_parcel_pattern.format(year)))
        logging.debug(f"Found {file}")
        # non_residential_sqft is not available in the RTP2021 parcel table
        usecols = ['parcel_id','deed_restricted_units','preserved_units','subsidized_units','residential_units','inclusionary_units',
                    'hhq1','hhq2','hhq3','hhq4','tothh','totemp', "RETEMPN", "MWTEMPN", "OTHEMPN","HEREMPN","FPSEMPN"]
        if rtp == "RTP2025":
            usecols.append('non_residential_sqft')
        parcel_df = pd.read_csv(file, usecols=usecols)
        logging.info("  Read {:,} rows from parcel file {}".format(len(parcel_df), file))
        logging.debug("Head:\n{}".format(parcel_df.head()))
        logging.debug("preserved_units.value_counts():\n{}".format(parcel_df['preserved_units'].value_counts(dropna=False)))

        if rtp == "RTP2025":
            # add geography crosswalk for zoning categories
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2025_geography_crosswalk_df,
                how      = "left",
                left_on  = "parcel_id",
                right_on = "PARCEL_ID",
                validate = "one_to_one"
            )
            logging.debug("Head after merge with rtp2025_geography_crosswalk_df:\n{}".format(parcel_df.head()))
            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

            parcel_df = pd.merge(
                left     = parcel_df,
                right    = pba50_geography_crosswalk_df,
                how      = "left",
                left_on  = "parcel_id",
                right_on = "PARCEL_ID",
                validate = "one_to_one"
            )
            logging.debug("Head after merge with pba50_geography_crosswalk_df:\n{}".format(parcel_df.head()))
            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

            # add tract lookup for tract categories
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2025_tract_crosswalk_df,
                how      = "left",
                on       = "parcel_id",
                validate = "one_to_one"
            )

            # add transit service area lookups
            # logging.info("Columns in rtp2025_transit_service_df: ", rtp2025_transit_service_df.columns, rtp2025_transit_service_df.index.name)
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2025_transit_service_df,
                how      = "left",
                on       = "parcel_id",
                #right_on = "PARCEL_ID", # not needed with the current crosswalk
                validate = "one_to_one"
            )

            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
            logging.debug("Head after merge with rtp2025_transit_service_df:\n{}".format(parcel_df.head()))

            # add area_type (urban/suburban/rural) and superdistrict lookups
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2025_parcel_taz_crosswalk_df,
                how      = "left",
                on       = "parcel_id",
                #right_on = "PARCEL_ID", # not needed with the current crosswalk
                validate = "one_to_one"
            )

            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
            logging.debug("Head after merge with rtp2025_parcel_taz_crosswalk_df:\n{}".format(parcel_df.head()))


            # add parcel lookup for 2020 urban area footprint
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2025_urban_area_crosswalk_df,
                how      = "left",
                on       = "parcel_id",
                validate = "one_to_one"
            )
            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
            logging.debug("Head after merge with rtp2025_urban_area_crosswalk_df:\n{}".format(parcel_df.head()))

            # add parcel sea level rise inundation based on the Plan scenario
            this_modelrun_alias = classify_runid_alias(modelrun_alias)
            if this_modelrun_alias == "NP":
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = rtp2025_np_parcel_inundation_df,
                    how      = "left",
                    on       = "parcel_id",
                    validate = "one_to_one"
                )
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
                logging.debug("Head after merge with rtp2025_np_parcel_inundation_df:\n{}".format(parcel_df.head()))
            elif this_modelrun_alias == "DBP":
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = rtp2025_dbp_parcel_inundation_df,
                    how      = "left",
                    on       = "parcel_id",
                    validate = "one_to_one"
                )
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
                logging.debug("Head after merge with rtp2025_dbp_parcel_inundation_df:\n{}".format(parcel_df.head()))

            # rtp2025_tract_crosswalk_df.columns should all be ints -- convert
            cols_int64 = ['tract10','tract20']
            cols_int   = ['tract20_epc','tract20_growth_geo','tract20_tra','tract20_hra','tract10_DispRisk','in_urban_area']
            fill_cols  = {col:-1 for col in cols_int64+cols_int}
            logging.debug(fill_cols)
            parcel_df.fillna(fill_cols, inplace=True)
            parcel_df[cols_int64] = parcel_df[cols_int64].astype('int64')
            parcel_df[cols_int] = parcel_df[cols_int].astype(int)
            logging.debug("Head after int type conversion:\n{}".format(parcel_df.head()))
            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

        if rtp == "RTP2021":
            # if it's already here, remove -- we're adding from a single source
            if 'fbpchcat' in parcel_df.columns:
                parcel_df.drop(columns=['fbpchcat'], inplace=True)

            # join to get fbpchcat and the zoning columns (gg_id, tra_id, hra_id, dis_id)
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2021_geography_crosswalk_df,
                how      = "left",
                left_on  = "parcel_id",
                right_on = "PARCEL_ID",
                validate = "one_to_one"
            )
            assert('fbpchcat' in parcel_df.columns)

            # add TAZ1454 and superdistrict columns
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = parcel_taz_sd_crosswalk_df,
                how      = "left",
                on       = "parcel_id",
                validate = "one_to_one"
            )

            # add transit service area lookups
            # logging.info("Columns in rtp2025_transit_service_df: ", rtp2025_transit_service_df.columns, rtp2025_transit_service_df.index.name)
            parcel_df = pd.merge(
                left     = parcel_df,
                right    = rtp2025_transit_service_df,
                how      = "left",
                on       = "parcel_id",
                #right_on ="PARCEL_ID",
                validate = "one_to_one"
            )

            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
            logging.debug("Head after merge with rtp2025_tract_crosswalk_df:\n{}".format(parcel_df.head()))

            # add parcel sea level rise inundation *input* based on the scenario
            this_modelrun_alias = classify_runid_alias(modelrun_alias)
            if this_modelrun_alias == "NP":
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = rtp2021_np_parcel_inundation_df,
                    how      = "left",
                    on       = "parcel_id",
                    validate = "one_to_one"
                )
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
                logging.debug("Head after merge with rtp2021_np_parcel_inundation_df:\n{}".format(parcel_df.head()))
            else:
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = rtp2021_fbp_parcel_inundation_df,
                    how      = "left",
                    on       = "parcel_id",
                    validate = "one_to_one"
                )
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
                logging.debug("Head after merge with rtp2021_fbp_parcel_inundation_df:\n{}".format(parcel_df.head()))

            # Merge the tract and coc crosswalks
            parcel_df = parcel_df.merge(rtp2021_tract_crosswalk_df, on="parcel_id", how="left")
            logging.debug("parcel_df after first merge with tract crosswalk:\n{}".format(parcel_df.head(30)))

            parcel_df = parcel_df.merge(rtp2021_pda_crosswalk_df, on="parcel_id", how="left")
            logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

            # Retain only a subset of columns after merging
            columns_to_keep = ['parcel_id', 'tract10', 'fbpchcat', 
                                'gg_id', 'tra_id', 'hra_id', 'dis_id', 'ppa_id', 'eir_coc_id','jurisdiction',
                                'zone_id', 'county', 'superdistrict',
                                'hhq1', 'hhq2', 'hhq3', 'hhq4', 
                                'tothh', 'totemp',
                                'deed_restricted_units', 'residential_units', 'preserved_units',
                                'pda_id_pba50_fb',
                                # employment
                                'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN',
                                # tract-level columns
                                'tract10_epc', 'tract10_DispRisk', 'tract10_hra', 'tract10_growth_geo', 'tract10_tra',
                                
                                # transit-related columns
                                #'area_type','Service_Level_np_cat5', 'Service_Level_fbp_cat5', 'Service_Level_current_cat5',
                                
                                # use after may 3 2024
                                'np','cur','dbp',
                                
                                # sea level rise column
                                "inundation"]

            parcel_df = parcel_df[columns_to_keep]
            logging.debug("parcel_df:\n{}".format(parcel_df.head(30)))

        modelrun_data[year]['parcel'] = parcel_df

    # Load county summaries
    for year in sorted(modelrun_data.keys()):
        logging.debug("Looking for geographic summaries matching {}".format(geo_summary_pattern.format(year)))
        file = next(run_directory_path.glob(geo_summary_pattern.format(year)))
        logging.debug(f"Found {file}")
        geo_summary_df = pd.read_csv(file)
        logging.info("  Read {:,} rows from geography summary {}".format(len(geo_summary_df), file))
        logging.debug("Head:\n{}".format(geo_summary_df))
        modelrun_data[year]['county'] = geo_summary_df

        if rtp=="RTP2021":
            # rename some columns to be consistent with RTP2025
            modelrun_data[year]['county'].rename(columns={
                'COUNTY_NAME'   :'county',
                'HHINCQ1'       :'hhincq1',
                'HHINCQ2'       :'hhincq2',
                'HHINCQ3'       :'hhincq3',
                'HHINCQ4'       :'hhincq4',
                'TOTEMP'        :'totemp',
                'TOTHH'         :'tothh',
            }, inplace=True)
            logging.debug(f"{modelrun_data[year]['county'].head()=}")

    # Load taz summaries
    # This is only necessary for RTP2025 / healthy.urban_park_acres()
    #         and superdistrict-based jobs/housing summaries
    if rtp == "RTP2025":
        for year in sorted(modelrun_data.keys()):
            logging.debug("Looking for taz1 summaries matching {}".format(taz1_summary_pattern.format(year)))
            file = next(run_directory_path.glob(taz1_summary_pattern.format(year)))
            logging.debug(f"Found {file}")
            taz1_summary_df = pd.read_csv(file, usecols=['TAZ','COUNTY','SD','TOTHH','TOTEMP','TOTPOP'],
                                          dtype={'SD':str}) # consider SD as a string
            taz1_summary_df.rename(columns={'TAZ':'TAZ1454'}, inplace=True)
            logging.info("  Read {:,} rows from taz summary {}".format(len(taz1_summary_df), file))
            logging.debug("Head:\n{}".format(taz1_summary_df))

            # there is a second TAZ level summary data with more variables than those used by
            # the tm, including building derived data such as sqft. We need office spaces and jobs and vacancy.
            # However, these variables were added recently.

            logging.debug("Looking for taz interim summaries matching {}".format(taz1_interim_summary_pattern.format(year)))
            file = next(run_directory_path.glob(taz1_interim_summary_pattern.format(year)))
            logging.debug(f"Found {file}")
            taz1_interim_summary_df = pd.read_csv(file)
            taz1_interim_summary_df.rename(columns={'TAZ':'TAZ1454'}, inplace=True)

            # check
            taz_interim_cols = [
                #"TAZ",
                "non_residential_sqft",
                "non_residential_sqft_office",
                "job_spaces",
                "job_spaces_office",
                "non_residential_vacancy",
                "non_residential_vacancy_office"
                
            ]
            taz_interim_keep_cols = [x for x in taz_interim_cols if x in taz1_interim_summary_df.columns ]
            if len(taz1_interim_summary_df)>0:
                logging.debug("Columns in taz1_interim_summary_df: {}".format(taz1_interim_summary_df.columns))
                logging.debug("Columns to keep: {}".format(taz_interim_keep_cols))
                # assert(all(x in taz1_interim_summary_df.columns for x in taz_interim_cols))
                taz1_interim_summary_df = taz1_interim_summary_df[['TAZ1454']+taz_interim_keep_cols]

                logging.info("  Read {:,} rows from taz interim summary {}".format(len(taz1_interim_summary_df), file))
                logging.debug("Head:\n{}".format(taz1_interim_summary_df))

                # then combine with the first TAZ level summary data
                taz1_summary_df = pd.merge(
                    left   = taz1_summary_df,
                    right  = taz1_interim_summary_df,
                    on       = "TAZ1454",
                    how      = "left",
                    validate = "one_to_one"
                )

            taz1_summary_df = pd.merge(
                left     = taz1_summary_df,
                right    = rtp2025_taz_crosswalk_df,
                on       = "TAZ1454",
                how      = "left",
                validate = "one_to_one"
            )
            logging.debug("Head:\n{}".format(taz1_summary_df))
            modelrun_data[year]['TAZ1454'] = taz1_summary_df

    # Interpolate to 2023 base year
    if (rtp == "RTP2025") and (not no_interpolate) and (not skip_base_year):
        logging.info("Interpolating to 2023 base year")
        modelrun_data[2023] = {}
        for geog in modelrun_data[2020].keys():  # could get geog and 2020 df via .items() but I think this is clearer if more verbose
            t1, t2 = 2020, 2025
            df1 = modelrun_data[t1][geog]
            df2 = modelrun_data[t2][geog]

            df = df1.copy()
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    # Long way to write 3/5 but maybe it'll pay off in future... :)
                    df[col] = df1[col] + ((2023 - t1) / (t2 - t1))*(df2[col] - df1[col])

            modelrun_data[2023][geog] = df

        logging.info("Deleting 2020 and 2025 data")
        del modelrun_data[2020]
        del modelrun_data[2025]

    logging.debug("modelrun_data:\n{}".format(modelrun_data))
    return modelrun_data


def classify_runid_alias(runid_alias):

    import re

    """
    The runid_alias strings capture the no project vs project distinction, but they
    are sometimes padded with other stuff. This hack function classifies the runid_alias 
    into one of four options:
    - DBP, short for draft blueprint, and NP, for no project, FBP for final blueprint.
    - Anything else is coded as unknown.

    Args:
        runid_alias: the run identifier from the run log

    Returns:
        A string, either "DBP" or "NP".
    """
    text = runid_alias.lower()  # Convert to lowercase for case-insensitive matching
    if ("draft" in text and "blueprint" in text) or re.search(r"dbp", text):
        return "DBP"
    elif (
        "no project" in text  # Capture all "No Project" variations
        or "np" in text  # Capture "NP" at the beginning (e.g., NP_Final)
        or text.startswith("no_")  # Capture variations starting with "no_"
        or "noproject" in text
    ):  # Explicit check for "noproject"
        return "NP"
    elif ("final" in text and "blueprint" in text) or re.search(r"fbp|final", text):
        return "FBP"
    else:
        return "Unknown"  # Default to Unknown
