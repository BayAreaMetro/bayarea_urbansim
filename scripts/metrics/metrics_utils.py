import pandas as pd
import logging
from datetime import datetime

# make global so we only read once
rtp2025_geography_crosswalk_df = pd.DataFrame() # parcel -> zoning categories (epc, displacement, growth geog, hra, tra)
rtp2025_taz_crosswalk_df       = pd.DataFrame() # parcel -> tract/taz

rtp2021_tract_crosswalk_df     = pd.DataFrame() # parcel -> tracts, including coc/epc, displacement, growth geography, HRA, TRA
rtp2021_pda_crosswalk_df       = pd.DataFrame() # parcel -> PDA (pda_id_pba50_fb)
rtp2021_geography_crosswalk_df = pd.DataFrame() # parcel -> parcel category (fbpchcat -> growth geog, hra, tra)

PARCEL_AREA_FILTERS = {
    'RTP2021': {
            'HRA'      : lambda df: df['hra_id'] == 'HRA',
            'TRA'      : lambda df: df['tra_id'] != 'NA',  # note this is the string NA
            'HRAandTRA': lambda df: (df['tra_id'] != 'NA') & (df['hra_id'] == 'HRA'),
            'GG'       : lambda df: df['gg_id'] == 'GG',
            'PDA'      : lambda df: pd.notna(df['pda_id_pba50_fb']),
            'EPC'      : lambda df: df['tract_epc'] == 1,
            'Region'   : None
    },
    'RTP2025': {
            'HRA'      : lambda df: df['hra_id'] == 'HRA',
            'TRA'      : lambda df: df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']),
            'HRAandTRA': lambda df: (df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])) & (df['hra_id'] == 'HRA'),
            'GG'       : lambda df: df['gg_id'] == 'GG',
            'PDA'      : lambda df: pd.notna(df['pda_id']), # this should be modified
            'EPC'      : lambda df: df['epc_id'] == 'EPC',
            'Region'   : None
    }
}

# --------------------------------------
# Data Loading Based on Model Run Plan
# --------------------------------------
def load_data_for_runs(rtp, METRICS_DIR, run_directory_path, modelrun_alias):
    """
    Reads crosswalk data as well as core summary and geographic summary data for the given BAUS model run
    for both the base year and the horizon year (which varies based on the rtp)

    DataFrames into two lists: one for core summaries and one for geographic summaries.

    Parameters:
    - rtp (str): one of RTP2021 or RTP2025
    - METRICS_DIR (str): metrics directory for finding crosswalks
    - run_directory_path (pathlib.Path): path for model run output files
    - modelrun_alias (str): alias for the model run. e.g. 'No Project', 'DBP, etc.

    Returns:
    - dict with year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    
    Both lists include DataFrames for files that match the year-specific patterns, 
    assuming files for both target years are present.
    """
    # make global so we only read once
    global rtp2025_geography_crosswalk_df
    global rtp2025_taz_crosswalk_df
    global rtp2021_geography_crosswalk_df
    global rtp2021_tract_crosswalk_df
    global rtp2021_pda_crosswalk_df

    # year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    modelrun_data = {}
    if rtp == "RTP2025":
        if len(rtp2025_geography_crosswalk_df) == 0:
            PARCEL_CROSSWALK_FILE = "M:/urban_modeling/baus/BAUS Inputs/basis_inputs/crosswalks/parcels_geography_2024_02_14.csv"
            rtp2025_geography_crosswalk_df = pd.read_csv(PARCEL_CROSSWALK_FILE, usecols=['PARCEL_ID','dis_id','tra_id','gg_id','pda_id','hra_id','epc_id','ugb_id'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_geography_crosswalk_df), PARCEL_CROSSWALK_FILE))
            logging.debug("  rtp2025_geography_crosswalk_df.head():\n{}".format(rtp2025_geography_crosswalk_df.head()))

        # tract/taz
        if len(rtp2025_taz_crosswalk_df) == 0:
            TAZ_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "parcel_tract_crosswalk.csv"
            rtp2025_taz_crosswalk_df = pd.read_csv(TAZ_CROSSWALK_FILE, usecols=['parcel_id','zone_id','tract_id'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2025_taz_crosswalk_df), TAZ_CROSSWALK_FILE))
            logging.info("  rtp2025_taz_crosswalk_df: {:,} tract_id values, {:,} zone_id values".format(
                len(rtp2025_taz_crosswalk_df.tract_id.unique()),
                len(rtp2025_taz_crosswalk_df.zone_id.unique())))
            logging.debug("  rtp2025_taz_crosswalk_df.head():\n{}".format(rtp2025_taz_crosswalk_df.head()))
            # Note that tracts don't map to only one zone_id; we'll need to deal with this later
            tract_summary_df = rtp2025_taz_crosswalk_df.groupby(['tract_id']).agg(
                taz_count     = pd.NamedAgg(column="zone_id",   aggfunc="nunique"),
                parcel_count  = pd.NamedAgg(column="parcel_id", aggfunc="nunique")
            )
            logging.debug("tract_summary_df.describe():\n{}".format(tract_summary_df.describe()))
            # rename for remaining
            rtp2025_taz_crosswalk_df.rename(columns={'zone_id':'TAZ1454'}, inplace=True)

            # taz-based lookups
            TAZ_EPC_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "taz1454_epcPBA50plus_2024_02_23.csv"
            taz_epc_df = pd.read_csv(TAZ_EPC_CROSSWALK_FILE, usecols=['TAZ1454','taz_epc'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(taz_epc_df), TAZ_EPC_CROSSWALK_FILE))
            logging.debug("  taz_epc_df.head():\n{}".format(taz_epc_df.head()))
            rtp2025_taz_crosswalk_df = pd.merge(
                left     = rtp2025_taz_crosswalk_df,
                right    = taz_epc_df,
                on       = 'TAZ1454',
                how      = 'left',
                validate = 'many_to_one'
            )

            TAZ_GG_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "taz1454_ggPBA50plus_2024_02_23.csv"
            taz_gg_df = pd.read_csv(TAZ_GG_CROSSWALK_FILE, usecols=['TAZ1454', 'growth_geo'])
            taz_gg_df.rename(columns={'growth_geo':'taz_growth_geo'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(taz_gg_df), TAZ_GG_CROSSWALK_FILE))
            logging.debug("  taz_gg_df.head():\n{}".format(taz_gg_df.head()))
            rtp2025_taz_crosswalk_df = pd.merge(
                left     = rtp2025_taz_crosswalk_df,
                right    = taz_gg_df,
                on       = 'TAZ1454',
                how      = 'left',
                validate = 'many_to_one'
            )

            TAZ_TRA_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "taz1454_ggtraPBA50plus_2024_02_23.csv"
            taz_tra_df = pd.read_csv(TAZ_TRA_CROSSWALK_FILE, usecols=['TAZ1454', 'gg_tra'])
            taz_tra_df.rename(columns={'gg_tra':'taz_tra'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(taz_tra_df), TAZ_TRA_CROSSWALK_FILE))
            logging.debug("  taz_tra_df.head():\n{}".format(taz_tra_df.head()))
            rtp2025_taz_crosswalk_df = pd.merge(
                left     = rtp2025_taz_crosswalk_df,
                right    = taz_tra_df,
                on       = 'TAZ1454',
                how      = 'left',
                validate = 'many_to_one'
            )

            TAZ_HRA_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "taz1454_hraPBA50plus_2024_02_23.csv"
            taz_hra_df = pd.read_csv(TAZ_HRA_CROSSWALK_FILE, usecols=['TAZ1454','taz_hra'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(taz_hra_df), TAZ_HRA_CROSSWALK_FILE))
            logging.debug("  taz_hra_df.head():\n{}".format(taz_hra_df.head()))
            rtp2025_taz_crosswalk_df = pd.merge(
                left     = rtp2025_taz_crosswalk_df,
                right    = taz_hra_df,
                on       = 'TAZ1454',
                how      = 'left',
                validate = 'many_to_one'
            )

            # displacement risk - udp_file/udp_DR_df
            TRACT_DISPLACEMENT_FILE = METRICS_DIR / "metrics_input_files" / "udp_2017results.csv"
            tract_displacement_df = pd.read_csv(TRACT_DISPLACEMENT_FILE, usecols=['Tract','DispRisk'])
            tract_displacement_df.rename(columns={'Tract':'tract_id', 'DispRisk':'tract_DispRisk'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_displacement_df), TRACT_DISPLACEMENT_FILE))
            logging.debug("  tract_displacement_df.head():\n{}".format(tract_displacement_df.head()))
            rtp2025_taz_crosswalk_df = pd.merge(
                left     = rtp2025_taz_crosswalk_df,
                right    = tract_displacement_df,
                on       = 'tract_id',
                how      = 'left',
                validate = 'many_to_one'
            )

            # fillna with zero and make int
            rtp2025_taz_crosswalk_df.fillna(0, inplace=True)
            rtp2025_taz_crosswalk_df = rtp2025_taz_crosswalk_df.astype(int)

            logging.debug("final rtp2025_taz_crosswalk_df.head():\n{}".format(rtp2025_taz_crosswalk_df))

        # define analysis years
        modelrun_data[2020]  = {}
        modelrun_data[2050]  = {}
        parcel_pattern       = "core_summaries/*_parcel_summary_{}.csv"
        geo_summary_pattern  = "geographic_summaries/*_county_summary_{}.csv"
    elif rtp == "RTP2021":
        # these are all tract-based -- load into one dataframe
        if len(rtp2021_tract_crosswalk_df) == 0:

            # pba50_metrics.py called this parcel_tract_crosswalk_file/parcel_tract_crosswalk_df
            TRACT_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "parcel_tract_crosswalk.csv"
            rtp2021_tract_crosswalk_df = pd.read_csv(TRACT_CROSSWALK_FILE, usecols=['parcel_id','tract_id'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2021_tract_crosswalk_df), TRACT_CROSSWALK_FILE))
            logging.info("  rtp2021_tract_crosswalk_df.tract_id.unique count:{:,}".format(
                len(rtp2021_tract_crosswalk_df.tract_id.unique())
            ))
            logging.debug("  rtp2021_tract_crosswalk_df.head():\n{}".format(rtp2021_tract_crosswalk_df.head()))

            # pba50_metrics.py called this coc_flag_file/coc_flag_df
            COC_FLAG_FILE = METRICS_DIR / "metrics_input_files" / "COCs_ACS2018_tbl_TEMP.csv"
            tract_coc_df = pd.read_csv(COC_FLAG_FILE, usecols=['tract_id','coc_flag_pba2050'])
            tract_coc_df.rename(columns={'coc_flag_pba2050':'tract_epc'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_coc_df), COC_FLAG_FILE))
            logging.debug("  tract_coc_df.head():\n{}".format(tract_coc_df.head()))
            # merge it into rtp2021_tract_crosswalk_df
            rtp2021_tract_crosswalk_df = pd.merge(
                left     = rtp2021_tract_crosswalk_df,
                right    = tract_coc_df,
                on       = 'tract_id',
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
            tract_displacement_df.rename(columns={'Tract':'tract_id', 'DispRisk':'tract_DispRisk'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_displacement_df), TRACT_DISPLACEMENT_FILE))
            logging.debug("  tract_displacement_df.head():\n{}".format(tract_displacement_df.head()))
            # merge it into rtp2021_tract_crosswalk_df
            rtp2021_tract_crosswalk_df = pd.merge(
                left     = rtp2021_tract_crosswalk_df,
                right    = tract_displacement_df,
                on       = 'tract_id',
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
            tract_hra_df.rename(columns={'hra':'tract_hra', 'tra':'tract_tra', 'growth_geo':'tract_growth_geo'}, inplace=True)

            logging.debug("  tract_hra_df.head():\n{}".format(tract_hra_df.head()))
            # merge it into rtp2021_tract_crosswalk_df
            rtp2021_tract_crosswalk_df = pd.merge(
                left     = rtp2021_tract_crosswalk_df,
                right    = tract_hra_df,
                on       = 'tract_id',
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
            # columns are: parcel_id, tract_id, tract_epc, DispRisk, tract_hra, tract_growth_geo, tract_tra

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
            rtp2021_geography_crosswalk_df = pd.read_csv(GEOGRAPHY_CROSSWALK_FILE, usecols=['PARCEL_ID','fbpchcat'])
            logging.info("  Read {:,} rows from crosswalk {}".format(len(rtp2021_geography_crosswalk_df), GEOGRAPHY_CROSSWALK_FILE))
            logging.debug("  rtp2021_geography_crosswalk_df.head():\n{}".format(rtp2021_geography_crosswalk_df.head()))

            # Expand fbpchcat into component parts
            parcel_zoning_df = rtp2021_geography_crosswalk_df['fbpchcat'].str.extract(
                r'^(?P<gg_id>GG|NA)(?P<tra_id>tra1|tra2c|tra2b|tra2a|tra2|tra3a|tra3|NA)(?P<hra_id>HRA)?(?P<dis_id>DIS)?(?P<zone_remainder>.*)$')
            parcel_zoning_df['fbpchcat'] = rtp2021_geography_crosswalk_df['fbpchcat']
            logging.debug("parcel_zoning_df=\n{}".format(parcel_zoning_df.head(30)))

            # check if any are missed: if zone_remainder contains 'HRA' or 'DIS
            zone_re_problem_df = parcel_zoning_df.loc[parcel_zoning_df.zone_remainder.str.contains("HRA|DIS", na=False, regex=True)]
            logging.debug("zone_re_problem_df nrows={} dataframe:\n{}".format(len(zone_re_problem_df), zone_re_problem_df))

            # join it back to rtp2021_geography_crosswalk_df
            rtp2021_geography_crosswalk_df = pd.concat([rtp2021_geography_crosswalk_df,
                                                        parcel_zoning_df.drop(columns=['fbpchcat'])], axis='columns')
            logging.debug("  rtp2021_geography_crosswalk_df.head() after fbpchcat split:\n{}".format(
                rtp2021_geography_crosswalk_df.head()))

        # define analysis years
        modelrun_data[2015] = {}
        modelrun_data[2050] = {}
        parcel_pattern       = "*_parcel_data_{}.csv"
        geo_summary_pattern  = "*_county_summaries_{}.csv"

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
        parcel_file_list = run_directory_path.glob(modified_parcel_pattern.format(year))
        for file in parcel_file_list:
            parcel_df = pd.read_csv(
                file, usecols=['parcel_id','deed_restricted_units','preserved_units','subsidized_units','residential_units','inclusionary_units',
                               'hhq1','hhq2','hhq3','hhq4','tothh','totemp']) 
            logging.info("  Read {:,} rows from parcel file {}".format(len(parcel_df), file))
            logging.debug("Head:\n{}".format(parcel_df))
            logging.debug("preserved_units.value_counts():\n{}".format(parcel_df['preserved_units'].value_counts(dropna=False)))

            if rtp=="RTP2025":
                # add geography crosswalk for zoning categories
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = rtp2025_geography_crosswalk_df,
                    how      = "left",
                    left_on  = "parcel_id",
                    right_on = "PARCEL_ID",
                    validate = "one_to_one"
                )
                logging.debug("Head after merge with rtp2025_geography_crosswalk_df:\n{}".format(parcel_df))
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))
            
                # add taz crosswalk for tract/taz lookups
                # add geography crosswalk for zoning categories
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = rtp2025_taz_crosswalk_df,
                    how      = "left",
                    on       = "parcel_id",
                    validate = "one_to_one"
                )
                logging.debug("Head after merge with rtp2025_taz_crosswalk_df:\n{}".format(parcel_df))
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

                # TODO: Tracts classifications are needed for metrics_diverse.gentrify_displacement_tracts()
                # TODO: I think the below logic doesn't work because it's using households, and it shouldn't change
                # TODO: based on the output; it should be constant across all scenarios.
                # TODO: I think this should be removed and the tract-level crosswalk should be made externally and just read.
                # TODO: Flagged in Create tract-level crosswalks for epc, growth_geo, hra, tra
                # TODO: https://app.asana.com/0/0/1207017785853508/f
                # for RTP2025, we have taz_epc,taz_growth_geo,taz_tra,taz_hra and need to convert to tract versions
                for taz_var in ['taz_epc','taz_growth_geo','taz_tra','taz_hra']:
                    tract_var = taz_var.replace("taz_","tract_")
                    logging.info(f"Converting {taz_var} to {tract_var}")
                    tract_taz_df = parcel_df.groupby(['tract_id',taz_var]).agg(
                        tothh = pd.NamedAgg(column="tothh", aggfunc="sum"),
                    ).reset_index(drop=False)
                    logging.debug("tract_taz_df.head(50):\n{}".format(tract_taz_df.head(50)))
                    # cols: tract_id, taz_var, tothh
                    idx = tract_taz_df.groupby('tract_id')['tothh'].idxmax()
                    # idx: same length: tract/taz, value = index with max tothh
                    logging.debug("idx.head(50):\n{}".format(idx.head(50)))
                    # adds colun, tract_var, which is set to taz_var if tothh is bigger for this value, NaN otherwise
                    tract_taz_df[tract_var] = tract_taz_df.loc[idx, taz_var]
                    # summarize back to tract only - columns are now tract_id, tract_var
                    tract_df = tract_taz_df.groupby('tract_id').agg({taz_var:'first'}).reset_index(drop=False)
                    tract_df = tract_df.astype(int)
                    tract_df.rename(columns={taz_var:tract_var},inplace=True)
                    logging.debug("tract_df:\n{}".format(tract_df.head(50)))

                    # merge back to parcel_df
                    parcel_df = pd.merge(
                        left     = parcel_df,
                        right    = tract_df,
                        on       = 'tract_id',
                        how      = 'left',
                        validate = 'many_to_one'
                    )
                logging.debug("After adding tract vars from taz vars, parcel_df.dtypes:\n{}".format(parcel_df.dtypes))


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

                # Merge the tract and coc crosswalks
                parcel_df = parcel_df.merge(rtp2021_tract_crosswalk_df, on="parcel_id", how="left")
                logging.debug("parcel_df after first merge with tract crosswalk:\n{}".format(parcel_df.head(30)))

                parcel_df = parcel_df.merge(rtp2021_pda_crosswalk_df, on="parcel_id", how="left")
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

                # Retain only a subset of columns after merging
                columns_to_keep = ['parcel_id', 'tract_id', 'fbpchcat', 
                                   'gg_id', 'tra_id', 'hra_id', 'dis_id',
                                   'hhq1', 'hhq2', 'hhq3', 'hhq4', 
                                   'tothh', 'totemp',
                                   'deed_restricted_units', 'residential_units', 'preserved_units',
                                   'pda_id_pba50_fb',
                                   # tract-level columns
                                   'tract_epc', 'tract_DispRisk', 'tract_hra', 'tract_growth_geo', 'tract_tra']
                parcel_df = parcel_df[columns_to_keep]
                logging.debug("parcel_df:\n{}".format(parcel_df.head(30)))


            modelrun_data[year]['parcel'] = parcel_df

    # Load geographic summaries
    for year in sorted(modelrun_data.keys()):
        logging.debug("Looking for geographic summaries matching {}".format(geo_summary_pattern.format(year)))
        geo_summary_file_list = run_directory_path.glob(geo_summary_pattern.format(year))
        for file in geo_summary_file_list:
            geo_summary_df = pd.read_csv(file)
            logging.info("  Read {:,} rows from geography summary {}".format(len(geo_summary_df), file))
            logging.debug("Head:\n{}".format(geo_summary_df))
            modelrun_data[year]['county'] = geo_summary_df

    logging.debug("modelrun_data:\n{}".format(modelrun_data))
    return modelrun_data

