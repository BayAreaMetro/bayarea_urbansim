import pandas as pd
import logging
from datetime import datetime

# make global so we only read once
parcel_crosswalk_df    = pd.DataFrame()
geography_crosswalk_df = pd.DataFrame()
tract_crosswalk_df     = pd.DataFrame()
pda_crosswalk_df       = pd.DataFrame()
# --------------------------------------
# Data Loading Based on Model Run Plan
# --------------------------------------
def load_data_for_runs(rtp, METRICS_DIR, run_directory_path):
    """
    Reads crosswalk data as well as core summary and geographic summary data for the given BAUS model run
    for both the base year and the horizon year (which varies based on the rtp)

    DataFrames into two lists: one for core summaries and one for geographic summaries.

    Parameters:
    - rtp (str): one of RTP2021 or RTP2025
    - METRICS_DIR (str): metrics directory for finding crosswalks
    - run_directory_path (pathlib.Path): path for model run output files

    Returns:
    - dict with year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    
    Both lists include DataFrames for files that match the year-specific patterns, 
    assuming files for both target years are present.
    """
    # make global so we only read once
    global parcel_crosswalk_df
    global geography_crosswalk_df
    global tract_crosswalk_df
    global pda_crosswalk_df

    # year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    modelrun_data = {}
    if rtp == "RTP2025":
        if len(parcel_crosswalk_df) == 0:
            PARCEL_CROSSWALK_FILE = "M:/urban_modeling/baus/BAUS Inputs/basis_inputs/crosswalks/parcels_geography_2024_02_14.csv"
            parcel_crosswalk_df = pd.read_csv(PARCEL_CROSSWALK_FILE)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(parcel_crosswalk_df), PARCEL_CROSSWALK_FILE))
            logging.debug("  parcel_crosswalk_df.head():\n{}".format(parcel_crosswalk_df.head()))

        # define analysis years
        modelrun_data[2020]  = {}
        modelrun_data[2050]  = {}
        core_summary_pattern = "core_summaries/*_parcel_summary_{}.csv"
        geo_summary_pattern  = "geographic_summaries/*_county_summary_{}.csv"
    elif rtp == "RTP2021":
        if len(geography_crosswalk_df) == 0:
            GEOGRAPHY_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "COCs_ACS2018_tbl_TEMP.csv"
            geography_crosswalk_df = pd.read_csv(GEOGRAPHY_CROSSWALK_FILE)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(geography_crosswalk_df), GEOGRAPHY_CROSSWALK_FILE))
            logging.debug("  geography_crosswalk_df.head():\n{}".format(geography_crosswalk_df.head()))

        if len(tract_crosswalk_df) == 0:
            TRACT_CROSSWALK_FILE = METRICS_DIR / "metrics_input_files" / "parcel_tract_crosswalk.csv"
            tract_crosswalk_df = pd.read_csv(TRACT_CROSSWALK_FILE)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(tract_crosswalk_df), TRACT_CROSSWALK_FILE))
            logging.debug("  tract_crosswalk_df.head():\n{}".format(tract_crosswalk_df.head()))

        if len(pda_crosswalk_df) == 0:
            # the old script called this "parcel_GG_newxwalk_file"
            PDA_CROSSWALK_FILE =  METRICS_DIR / "metrics_input_files" / "parcel_tra_hra_pda_fbp_20210816.csv"
            pda_crosswalk_df = pd.read_csv(PDA_CROSSWALK_FILE, usecols=['PARCEL_ID','pda_id_pba50_fb'])
            pda_crosswalk_df.rename(columns={'PARCEL_ID':'parcel_id'}, inplace=True)
            logging.info("  Read {:,} rows from crosswalk {}".format(len(pda_crosswalk_df), PDA_CROSSWALK_FILE))
            logging.debug("  pda_crosswalk_df.head():\n{}".format(pda_crosswalk_df.head()))

        # define analysis years
        modelrun_data[2015] = {}
        modelrun_data[2050] = {}
        core_summary_pattern = "*_parcel_data_{}.csv"
        geo_summary_pattern  = "*_county_summaries_{}.csv"

    else:
        raise ValueError(f"Unrecognized plan: {rtp}")
    
    # Load parcels summaries
    for year in sorted(modelrun_data.keys()):
        logging.debug("Looking for core summaries matching {}".format(core_summary_pattern.format(year)))
        core_summary_file_list = run_directory_path.glob(core_summary_pattern.format(year))
        for file in core_summary_file_list:
            # TODO: consider adding usecols when all metrics are defined to increase the speed
            parcel_df = pd.read_csv(file) 
            logging.info("  Read {:,} rows from parcel file {}".format(len(parcel_df), file))
            logging.debug("Head:\n{}".format(parcel_df))

            if rtp=="RTP2025":
                parcel_df = pd.merge(
                    left     = parcel_df,
                    right    = parcel_crosswalk_df,
                    how      = "left",
                    left_on  = "parcel_id",
                    right_on = "PARCEL_ID",
                    validate = "one_to_one"
                )
                logging.debug("Head after merge with parcel_crosswalk:\n{}".format(parcel_df))
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

            if rtp == "RTP2021":
                zoning_column = None
                if 'fbpchcat' in parcel_df.columns:
                    zoning_column = 'fbpchcat'
                elif 'eirzoningmodcat' in parcel_df.columns:
                    zoning_column = 'eirzoningmodcat'
                else:
                    logging.warning(f"'Neither 'fbpchcat' nor 'eirzoningmodcat' found in DataFrame for model run. Skipping this DataFrame.")

                if zoning_column:
                    logging.debug("zoning_column {} unique values:\n{}".format(zoning_column, parcel_df[zoning_column].unique()))
                    # Expand the zoning column into component parts
                    parcel_zoning_df = parcel_df[zoning_column].str.extract(
                        r'^(?P<gg_id>GG|NA)(?P<tra_id>tra1|tra2c|tra2b|tra2a|tra2|tra3a|tra3|NA)(?P<hra_id>HRA)?(?P<dis_id>DIS)?(?P<zone_remainder>.*)$')
                    parcel_zoning_df[zoning_column] = parcel_df[zoning_column]
                    logging.debug("parcel_zoning_df=\n{}".format(parcel_zoning_df.head(500)))

                    # check if any are missed: if zone_remainder contains 'HRA' or 'DIS
                    zone_re_problem_df = parcel_zoning_df.loc[parcel_zoning_df.zone_remainder.str.contains("HRA|DIS", na=False, regex=True)]
                    logging.debug("zone_re_problem_df nrows={} dataframe:\n{}".format(len(zone_re_problem_df), zone_re_problem_df))

                    # join it back to parcel_df
                    parcel_df = pd.concat([parcel_df, parcel_zoning_df.drop(columns=[zoning_column])], axis='columns')

                # Merging using the tract and geography crosswalks
                parcel_df = parcel_df.merge(tract_crosswalk_df, on="parcel_id", how="left")
                logging.debug("parcel_df after first merge with tract crosswalk:\n{}".format(parcel_df.head(30)))

                parcel_df = parcel_df.merge(geography_crosswalk_df, on="tract_id", how="left")
                logging.debug("parcel_df after second merge with geography crosswalk:\n{}".format(parcel_df.head(30)))

                parcel_df = parcel_df.merge(pda_crosswalk_df, on="parcel_id", how="left")
                logging.debug("parcel_df.dtypes:\n{}".format(parcel_df.dtypes))

                # Retain only a subset of columns after merging
                columns_to_keep = ['parcel_id', 'tract_id', zoning_column, 
                                   'gg_id', 'tra_id', 'hra_id', 'dis_id',
                                   'hhq1', 'hhq2', 'hhq3', 'hhq4', 
                                   'tothh', 'totemp',
                                   'deed_restricted_units', 'residential_units', 'preserved_units',
                                   'geoid', 'tot_pop', 'coc_flag_pba2050', 'coc_class', 'pda_id_pba50_fb']
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

# -----------------------------------
# Metrics Results Saving Utility
# -----------------------------------
def save_metric_results(df, metric_name, modelrun_id, modelrun_alias, output_path):
    filename = f"{metric_name}_{modelrun_id}_{modelrun_alias}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
    filepath = output_path / "Metrics" / filename
    df.to_csv(filepath, index=False)
    logging.info(f"Saved {metric_name} results to {filepath}")

# ----------------------------------
# Results Assembly for the Metrics
# ----------------------------------
def assemble_results_wide_format(modelrun_id, modelrun_alias, metrics_data):
    """
    Assemble metric results into a DataFrame with a wide format.

    Parameters:
    - modelrun_id: str, identifier for the model run.
    - modelrun_alias: str, a friendly or alias name for the model run.
    - metrics_data: dict, a dictionary where each key is a metric name and each value is a tuple or list containing:
        - A list of years (int)
        - A list of corresponding metric values (float or int)
    
    Returns:
    - pd.DataFrame with the assembled metric results in a wide format, where each metric has its own column,
      including 'modelrun_id', 'modelrun_alias', and 'year' as separate columns.
    """
    # Initialize an empty list to hold each row of our resulting DataFrame
    results_list = []

    # Iterate through each metric in the metrics_data
    for metric_name, (years, values) in metrics_data.items():
        # Assuming each metric has values for the same set of years, 
        # this loop will create a row for each year with the metric's value
        for year, value in zip(years, values):
            result = {
                "year": year,
                metric_name: value  # Dynamically name the column after the metric
            }
            results_list.append(result)
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(results_list)

    # Since multiple metrics can share the same year, we need to group by 'year' before pivoting
    # This step aggregates all metric values for the same year into a single row
    df_grouped = df.groupby('year', as_index=False).first()

    # Add 'modelrun_id' and 'modelrun_alias' to the DataFrame
    df_grouped['modelrun_id'] = modelrun_id
    # Concatenate 'year' and 'modelrun_alias'
    df_grouped['modelrun_alias'] = df_grouped['year'].astype(str) + " " + modelrun_alias
    
    # Drop the 'year' column as it's now redundant
    df_grouped = df_grouped.drop('year', axis=1)

    # Reorder columns to place 'modelrun_id', 'modelrun_alias' at the front
    cols = ['modelrun_id', 'modelrun_alias'] + [col for col in df_grouped if col not in ['modelrun_id', 'modelrun_alias']]
    df_wide = df_grouped[cols]
    
    return df_wide

def map_area_to_alias(area):
    area_aliases = {'HRA': "High-Resource Areas",
                    'EPC': "Equity Priority Communities",
                    'Region': "Regionwide"}
    return area_aliases.get(area, "Unknown Area")
