#!/usr/bin/env python
# coding: utf-8

import os
import logging
import yaml
from datetime import datetime
from pathlib import Path
from itertools import product

import pandas as pd
import geopandas as gpd


def logger_process(
        dir_path: Path,
        log_name: str = "connected_metric.log"
):

    # Create a unique log file path
    log_file = Path(dir_path, log_name)

    # Create the output directory if it doesn't exist
    # output_dir = Path(dir_path, "outputs")
    # output_dir.mkdir(parents=True, exist_ok=True)  # Create parent directories if needed

    # Configure logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Sset format for log messages
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )

    # Console handler with INFO level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler with DEBUG level
    fh = logging.FileHandler(log_file, mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info(f"Initializing log file at {log_file.as_posix()}")

    return logger

# function for forming the file path


def parcel_file_path(
        baus_run_details:dict,
        run_handle:str,
        year:str,
        logger:logging.Logger = None
        ) -> Path:
    """
    Returns the file path for the specified run and year.
    """

    logger.info(f"Getting file path for {run_handle} {year}")
    parcel_template = baus_run_details[run_handle]["parcel_template"][year]
    file_path = parcel_template.format(
        baus_run_details[run_handle]["mnemonic"], year)
    
    out_path = Path(
        baus_run_details[run_handle]["root_path"],
        baus_run_details[run_handle]["run_path"],
        file_path,
    )
    return out_path

# function for loading the file path into a df


def load_parcels(baus_run_details:dict,
                 run_handle:str,
                 year:str,
                 logger:logging.Logger = None, 
                 **csv_kwargs) -> pd.DataFrame:
    """
    Loads the parcels for the specified run and year.
    """

    # get path to relevant parcel data
    parcel_path = parcel_file_path(baus_run_details, run_handle, year, logger)

    logger.info(f"Loading parcels for {run_handle} {year}")

    # load the csv as a df - pass through kwargs such as use_cols
    parcels_df = pd.read_csv(parcel_path, **csv_kwargs)

    # adding a constant value for groupby convenience
    parcels_df["region"] = "Region"
    return parcels_df


def transit_service_area_share(
    parcel_output: pd.DataFrame,
    parcel_classes: pd.DataFrame,
    year: int,
    baus_scenario: str,
    variant: str,
    tableau_id: str
) -> pd.DataFrame:
    """Summarizes parcel data by transit service levels and geographic groupings.

    This function reads a DataFrame containing parcel information, prepares it for analysis,
    and then calculates summaries based on transit service levels, along with any geographic
    groupings, such as Equity Priority Communities (EPCs), High Resource Areas (HRAs) available.

    Args:
      parcel_output: A pandas DataFrame containing parcel level simulation run outputs.
      year: The simulation year for which the summaries are provided
      transit_scenario: The transit scenario to focus on (e.g., 'fbp' for final bluerprint). 
      The string is used to fetch the relevant transit scenario grouping column(s).

    Returns:
      A pandas DataFrame with summaries for various transit service levels, EPCs, HRAs, and area types.
    """

    # mapping relates the run type to which transit stop buffer universe is appropriate
    # e.g. final blueprint simulation runs should be matched with similar scenario transit stops / headways
    transit_scenario_mapping = {
        "NoProject": "np",
        "Plus": "fbp",
        "Final Blueprint": "fbp",
        "Draft Blueprint": "fbp",
        "Alt1": "fbp",
        "Alt2": "fbp",
        "Current": "cur",
    }

    # get the transit scenario to focus on (e.g., 'fbp' for final bluerprint)
    transit_scenario = transit_scenario_mapping[variant]

    # special case exception we can probably handle smoother
    transit_scenario = "cur" if int(year) == 2015 else transit_scenario

    # report shape of parcel_output df
    len_parcels = len(parcel_output)
    len_classifiers = len(parcel_classes)

    logger.info(f"Parcel output has {len_parcels:,} rows")
    logger.info(f"Parcel classifiers has {len_classifiers:,} rows")

    # Define columns containing values of interest
    val_cols = ["totemp", "RETEMPN", "MWTEMPN", "tothh"]

    # Fill missing values and convert specific columns to integers for consistency
    for col in val_cols:
        parcel_output[col] = parcel_output[col].fillna(0).round(0).astype(int)

    # Merge parcel_output data geographic classifiers / categorizations
    # Probably better to pass explicitly than to rely on global name space

    parcel_output_w_classifiers = parcel_classes.merge(
        parcel_output, left_on=["PARCEL_ID"], right_on=["parcel_id"]
    )
    len_merged = len(parcel_output_w_classifiers)
    logger.info(
        f"Merged parcel run data and parcel classifiers data has {len_merged} rows."
    )
    logger.info(f"The merged frame lost {len_parcels-len_merged:,} rows.")

    # convenience function for easy groupby percentages
    def pct(x):
        return x / x.sum()

    # convenience function for indicator specific groupby summaries, with variable groups
    def groupby_summaries(df, group_vars):

        grp_summary = (
            df.groupby(group_vars).agg(
                {
                    "tothh": "sum",
                    "hhq1": "sum",
                    "totemp": "sum",
                    "RETEMPN": "sum",
                    "MWTEMPN": "sum",
                }
            )
        )
        logger.info(
            f'Summarizing parcels with respect to: {"; ".join(group_vars)}')
        logger.info(f"{grp_summary.head()}")

        # this assumed the FIRST group level is the non-transit service area one!!
        # We are in other words getting the within-group distribution by transit service area
        # so, for HRAs, what is the share living in major transit areas; same for the region; same for

        # this_group_var = group_vars if isinstance(group_vars,str) else group_vars[0]
        grp_summary_shares = (
            grp_summary
            .groupby(level=group_vars[0],
                     group_keys=False)
            .apply(pct)
            .round(3)
        )

        return grp_summary_shares

    # Identify the passed scenario-specific columns (fbp,no project, current)
    # this returns different classifications for each - like the 5-way or 6-way service level (cat5, cat6)
    # several may be returned depending on how many are in the crosswalk
    # we summarize run data for each classification variable

    transit_svcs_cols = parcel_output_w_classifiers.filter(
        regex=transit_scenario
    ).columns.tolist()

    logger.info(
        f'Transit scenario specific classifier columns: {"; ".join(transit_svcs_cols)}'
    )

    container = {}

    # Now for the summaries - a variable (e.g. tothh) for a geographic area (e.g. region, hra, etc.)
    # is distributed by transit service area (e.g. cat5, cat6, etc.): what share of households
    # live in major transit areas? what share of jobs live in major transit areas?

    area_vars = {'region': "Region", 'is_epc24': 'CoCs',
                 'is_hra23': 'HRAs', 'area_type': 'area_type'}

    # loop through combinations of area types and transit service area classifications
    for combo in product(*[area_vars, transit_svcs_cols]):

        # run summary on this combination
        logger.info(f"Running parcel summaries for {'-'.join(combo)}")
        this_summary_servicelevel = groupby_summaries(
            parcel_output_w_classifiers, list(combo)
        )

        # new key, including year
        this_key = (year,) + combo
        container[this_key] = this_summary_servicelevel

    container_df = pd.concat(
        container,
        names=[
            "year",
            "area_grouping",          # i.e. the area concept
            # i.e.  the classification variable (5-way; 6-way)
            "transit_grouping",
            "area_detail",            # i.e. the area concept domain levels
            "service_level_detail",   # i.e. the headway / stop type
        ],
    )

    container_df = container_df.stack()
    container_df = (
        container_df.round(3)
        .reset_index(name="value")
        .rename(columns={f"level_{container_df.index.nlevels-1}": "variable"})
    )

    # recode a value so it works readily when concatenating / comparing with the old version
    transit_service_simplification_map = {
        "Major_Transit_Stop_only": "majorstop"}
    container_df.service_level_detail = container_df.service_level_detail.replace(
        transit_service_simplification_map
    )

    container_df["name"] = container_df.apply(
        lambda x: f'transitproximity_{x.service_level_detail.lower().replace("_only","")}_shareof_{x["variable"].replace("EMPN", "EMPNjobs")}_{str(x.area_detail)}',
        axis=1,
    )

    # TODO - probably don't need *that* many identifiers here - or in the yaml file

    container_df["metric"] = "C1"
    container_df["modelrunID"] = baus_scenario
    container_df["tableau_id"] = tableau_id
    container_df["blueprint"] = variant

    # finally, clean up area values a bit
    container_df['area_grouping'] = container_df.area_grouping.map(area_vars)
    
    return container_df


def format_for_tableau(pba50_combo_metrics: pd.DataFrame,
                       qrystring: str,
                       ) -> pd.DataFrame:
    """Formats the output for Tableau.

    Args:
        pba50_combo_metrics (pd.DataFrame): The DataFrame containing transit proximity metrics.
        qrystring (str): A query string to filter the DataFrame.

    Returns:
        pd.DataFrame: The formatted DataFrame.
    """
    logger.info(f"Formatting for Tableau")
    logger.info(f"Query string: {qrystring}")
    logger.info(f"Number of metrics: {len(pba50_combo_metrics)}")

    tableau_column_map = {  # 'run_handle':'modelrun_alias',
        "modelrun_alias": "modelrun_alias",
        "area_grouping": "area_grouping",
        "transit_grouping": "transit_grouping",
        "area_detail": "area_detail",
        "service_level_detail": "service_level_detail",
        "variable": "type_code",
        "value": "transitproximity_majorstop_shareof",
    }
    # tableau friendly names: high level
    tableau_type_code_map = {
        "MWTEMPN": "Jobs",
        "RETEMPN": "Jobs",
        "totemp": "Jobs",
        "hhq1": "Households",
        "tothh": "Households",
    }
    
    # tableau friendly names: detail
    tableau_type_subgroup_map = {
        "MWTEMPN": "Manufacturing and Warehousing Jobs",
        "RETEMPN": "Retail Jobs",
        "totemp": "All Jobs",
        "hhq1": "Households with Low Incomes",
        "tothh": "All Households",
    }

    pba50_combo_metrics["modelrun_alias"] = pba50_combo_metrics.apply(
        lambda x: f"{x.year} {x.run_handle}", axis=1
    )

    # first, we subset away most of the categories
    pba50_combo_metrics_for_tableau = pba50_combo_metrics.query(qrystring)

    logger.info(pba50_combo_metrics_for_tableau.run_handle.value_counts())
    
    # then we deal with the schema and domains
    metrics_tableau_schema = (
        pba50_combo_metrics_for_tableau[
        tableau_column_map.keys()
        ]
        .rename(columns=tableau_column_map)
        )
    
    # assign group name
    metrics_tableau_schema["type_group"] = metrics_tableau_schema.type_code.map(
        tableau_type_code_map
    )
    
    # assign detailed group name
    metrics_tableau_schema["type_subgroup"] = metrics_tableau_schema.type_code.map(
        tableau_type_subgroup_map
    )

    # prepare output schema
    id_cols = ["modelrun_alias", "type_code", "type_group", "type_subgroup"]
    id_extra_cols = [
        "area_grouping",
        "transit_grouping",
        "area_detail",
        "service_level_detail",
    ]
    val_col = ["transitproximity_majorstop_shareof"]
    return (
        metrics_tableau_schema[id_cols + id_extra_cols + val_col]
        .set_index(id_cols + id_extra_cols)
        .iloc[:, 0]
    )


if __name__ == "__main__":

    # #################################################################
    # Set paths and constants #########################################
    # #################################################################

    home_dir = Path.home()
    box_dir = Path(home_dir, "Box")

    # set the mounted path for M: drive
    # in my case, M:/ is mounted to /Volumes/Data/Models
    m_drive = "/Volumes/Data/Models" if os.name != "nt" else "M:/"

    metrics_path = Path(
        box_dir,
        "Plan Bay Area 2050+",
        "Performance and Equity",
        "Plan Performance",
        "Equity_Performance_Metrics",
        "PBA50_reproduce_for_QA"
    )
    # working_dir_path = os.path.abspath(".")
    # M has been unstable recently, so heading to Box for now.
    working_dir_path = Path(
        m_drive, 
        "Data", 
        "GIS layers",
        "JobsHousingTransitProximity",
        "update_2024"
    )

    working_dir_path = Path(
        box_dir,"Modeling and Surveys",
        "Urban Modeling",
        "Spatial",
        "transit",
        "transit_service_levels",
        "update_2024"
    )
    working_dir_path.mkdir(parents=True, exist_ok=True)

    # load parcels - to - whatever crosswalk, including transit service 
    # areas, HRAs, EPCs etc. These are used to classify parcels for summary.
    parcel_classes_gpd_path = Path(
        working_dir_path,
        "outputs",
        "p10_topofix_classified.parquet")
    
    p10_parcel_classified = gpd.read_parquet(parcel_classes_gpd_path)

    # #################################################################
    # Log file ########################################################
    # #################################################################

    YMD = datetime.now().strftime("%Y%m%d")
    logger = logger_process(working_dir_path, f"run_connected_metrics_{YMD}.log")

    # #################################################################
    # Get run data ####################################################
    # #################################################################
    
    logger.info("Loading yaml file with run info abstraction")

    # set the path for the run mapping yaml file containing deets on BAUS runs (shorthand, path, etc)
    baus_run_mapping_path = Path(
        metrics_path, 
        "meta",
        "baus_run_details.yaml" # this file should contain relevant run identifiers and paths for arbitrary vintages
        )

    # Load yaml file with run info abstraction
    # note that Nazanin has a similar concept in a csv file, for providing run details. Consolidate.
    
    def yaml_to_dict(baus_run_mapping_path):
        with open(baus_run_mapping_path) as ystream:
            try:
                baus_run_details = yaml.safe_load(ystream)
            except yaml.YAMLError as e:
                print(e)
        return baus_run_details
    
    baus_run_details = yaml_to_dict(baus_run_mapping_path)

    def subsetter(some_dict):
        new_dict = {}
        for k,v in some_dict.items():
            if v['include']:
                new_dict[k] = v
        return new_dict
    
    # just keep the runs flagged as include = true
    baus_run_details_sub = subsetter(baus_run_details)

    # dump dictionary to yaml
    # with open(baus_run_mapping_file, 'w',) as f :
    #         yaml.dump(baus_run_details,f,sort_keys=False)

    # #################################################################
    # Set paths and constants #########################################
    # #################################################################
            
    # Target output format and schema for tableau
    tableau_target_schema_new_file = Path(
        box_dir,
        "Plan Bay Area 2050+",
        "Performance and Equity",
        "Plan Performance",
        "Equity_Performance_Metrics",
        "PBA50_reproduce_for_QA",
        "metrics_connected1_transitproximity.csv",
    )
    # set target path for detailed breakdown
    tableau_target_schema_new_detail_file = Path(
        box_dir,
        "Plan Bay Area 2050+",
        "Performance and Equity",
        "Plan Performance",
        "Equity_Performance_Metrics",
        "PBA50_reproduce_for_QA",
        "metrics_connected1_transitproximity_detail.csv",
    )


    # #################################################################
    # Run the metrics for each simulation #############################
    # #################################################################

    # Run in batch mode - 
    # Loop through runs defined in the yaml, load data, and summarize each

    # we don't need that many columns from the parcel files - so save memory
    parcel_cols_keep = ["parcel_id", "tothh", "hhq1", "totemp", "RETEMPN", "MWTEMPN"]

    summaries_storage = {}
    for run_handle, vals in baus_run_details.items():
        logger.info(f"Processing {run_handle} 2050 data...")
        # load run data
        this_parcel_data = load_parcels(baus_run_details, run_handle, "2050", logger, usecols=parcel_cols_keep)

        # add classification and summarize run
        this_summary = transit_service_area_share(
            parcel_output=this_parcel_data,
            parcel_classes=p10_parcel_classified,
            year="2050",
            baus_scenario=vals["tm_id"],
            variant=vals["variant"],
            tableau_id=vals["tableau_id"],
        )
        summaries_storage[(run_handle, "2050")] = this_summary

        # we get ONE 2015 parcel dataset - we grab the one for FBA50_FBP / 182
        if run_handle == "PBA50_FBP":
            logger.info("Processing PBA50_FBP for 2015 for a base year dataset")
            # add base year to the mix
            this_parcel_data = load_parcels(baus_run_details, run_handle, "2015", logger, usecols=parcel_cols_keep)

            this_summary = transit_service_area_share(
                parcel_output=this_parcel_data,
                parcel_classes=p10_parcel_classified,
                year="2015",
                baus_scenario=vals["tm_id"],
                variant=vals["variant"],
                tableau_id=vals["tableau_id"],
            )
            summaries_storage[(run_handle, "2015")] = this_summary

    # combine batch here
    logger.info("Combining run summaries")
    pba50_combo_metrics = (
        pd.concat(summaries_storage, names=["run_handle", "yr", "oid"])
        .reset_index("oid", drop=True)
        .reset_index()
    )

    # #################################################################
    # Format for tableau ##############################################
    # #################################################################

    # subset / select just RTP2021 runs from bigger frame - runs in 
    # this id list should be in the yaml under the key `tableau_id`
    # paired with the relevant year as prefix
    RTP2021_RUNS = [
        "2050 PBA50_NP",
        "2015 PBA50_FBP",
        "2050 PBA50_FBP",
        "2050 PBA50_Alt1",
        "2050 PBA50_Alt2",
    ]

    # Note that the current tableau output only uses a subset of metrics
    # and doesn't provide much geographic detail, or service level 
    # detail. Those details are mostly useful for other purposes.

    # basic high level summary - regional totals, major stops only
    qry_string_cat5 = 'area_grouping=="Region" & transit_grouping.str.contains("cat5$") & service_level_detail=="majorstop"'
    updated_metrics_tableau_schema = format_for_tableau(
        pba50_combo_metrics, qrystring=qry_string_cat5
    )

    # here we subset for just a small subset of runs, for replication / testing
    rtp2021_metrics_replication = updated_metrics_tableau_schema.loc[RTP2021_RUNS]
    
    # here, we allow for more area and transit detail for a separate tableau view
    # https://10ay.online.tableau.com/#/site/metropolitantransportationcommission/views/metrics_Connected_reproduce_for_QA/C1TransitProximityBySubarea?:iid=1
    
    qry_string_cat6 = 'transit_grouping.str.contains("cat6$") '
    updated_metrics_tableau_schema_detail = format_for_tableau(
        pba50_combo_metrics, qrystring=qry_string_cat6
    )
    
    # these paths are defined at the top and is typically not expected to change, for tableau convenience
    rtp2021_metrics_replication.to_csv(tableau_target_schema_new_file)
    updated_metrics_tableau_schema_detail.to_csv(tableau_target_schema_new_detail_file)

    logger.info("Finished running connected detailed metric.")
    logger.handlers[0].close()