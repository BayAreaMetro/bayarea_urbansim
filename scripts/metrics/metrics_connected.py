# ===============================
# ====== Connected Metrics ======
# ===============================

import pandas as pd
import geopandas as gpd
import logging
import metrics_utils
import pathlib
from itertools import product


def transit_service_area_share(
                                rtp: str,
                                modelrun_alias: str, 
                                modelrun_id: str, 
                                modelrun_data: dict, 
                                output_path: str,
                                append_output: bool
                                ):

                 
    # TODO: we need a 2015 / 2020 / 2023 as well
    #year = 2050
    logging.info(f"Calculating connected for {modelrun_alias} / {modelrun_id}")
    logging.debug(f"Modelrun data years: {modelrun_data.keys()}")
    logging.debug(f"Modelrun data 2050 datasets: {modelrun_data[2050].keys()}")

    # convenience function for easy groupby percentages
    def pct(x):
        return x / x.sum()

    # convenience function for indicator specific groupby summaries, with variable groups
    def groupby_summaries(df, group_vars):

        grp_summary = (
            df.groupby(group_vars).agg(
                agg_mapping
            )
        )
        logging.debug(
            f'Summarizing parcels with respect to: {"; ".join(group_vars)}')
        logging.debug(f"{grp_summary.head()}")

        # this assumed the FIRST group level is the area and not the transit service area one!!
        # We are in other words getting the within-group distribution by transit service area
        # so, for HRAs, what is the share living in major transit areas vs outside; same for the region

        grp_summary_shares = (
            grp_summary
            .groupby(level=group_vars[0],
                    group_keys=False)
            .apply(pct)
            .round(3)
        )

        return grp_summary_shares

    # The transit_scenario_mapping relates the run type to which transit stop buffer universe is appropriate
    # e.g. final blueprint simulation runs should be matched with similar scenario transit stops / headways.
    
    # Left hand side is modelrunid_alias - we want to relate that to the specific transit stop map to use
    # where stops and headways will differ for np and dbp. The parcel crosswalk has different classifications
    # for np and dbp.

    #this_modelrun_alias = metrics_utils.classify_runid_alias(modelrun_alias)
    

    transit_scenario_mapping = {
        "NoProject": "np",
        "No Project": "np", # allowing for variations in input here
        "NP":'np',
        "Plus": "fbp",
        "Final Blueprint": "fbp",
        "Draft Blueprint": "fbp",
        "DBP": "fbp", # we map to FBP in the transit service class file
        "Alt1": "fbp",
        "Alt2": "fbp",
        "EIR Alt1":'fbp',
        "EIR Alt2":'fbp',
        "Current": "cur", # refers to existing conditions transit stops
    }

    # get the transit scenario to focus on (e.g., 'fbp' for final bluerprint)
    # we really need to have a distinct fbp/eir, etc designation from run id in the log
    transit_scenario = transit_scenario_mapping.get(modelrun_alias,'dbp')

    # Define columns containing values of interest - more could be added as long as it is present and numeric
    val_cols = ["totemp", "RETEMPN", "MWTEMPN","OTHEMPN","HEREMPN","FPSEMPN", "tothh", "hhq1"]
    # use for groupby later to say what do do with each col - we just sum them
    agg_mapping = {col: 'sum' for col in val_cols}


    SUMMARY_YEARS = sorted(modelrun_data.keys())

    # for storing yearly data
    container = {}


    for year in SUMMARY_YEARS:

        # set transit_scenario to cur (existing stops buffers) 
        transit_scenario = "cur" if int(year) in [2015, 2020, 2023] else transit_scenario

        parcel_output = modelrun_data[year]["parcel"].copy(deep=True)
        # report shape of parcel_output df
        len_parcels = len(parcel_output)

        logging.debug('Cols of parcels {} in connected func: {}'.format(year,parcel_output.columns))
        logging.debug(f"Parcel output has {len_parcels:,} rows")

        

        # Identify the passed scenario-specific columns (fbp no project, current)
        # this returns different classifications for each - like the 5-way or 6-way service level (cat5, cat6)
        # several may be returned depending on how many are in the crosswalk
        # we summarize run data for each classification variable

        transit_svcs_cols = parcel_output.filter(
            regex=transit_scenario
        ).columns.tolist()

        logging.debug(
            f'Transit scenario specific for {transit_scenario=} classifier columns: {"; ".join(transit_svcs_cols)}'
        )


        # Fill missing values and convert specific columns to integers for consistency
        for col in val_cols:
            parcel_output[col] = parcel_output[col].fillna(0).round(0).astype(int)

        # convenience function for easy groupby percentages

        # Now for the summaries - a variable (e.g. tothh) for a geographic area (e.g. region, hra, etc.)
        # is distributed by transit service area (e.g. cat5, cat6, etc.): what share of households
        # live in major transit areas? what share of jobs "live" in major transit areas?

        if rtp=="RTP2021":
            # RTP2021 was tract10-based
            area_vars = {'region': "Region", 'tract10_epc': 'CoCs',
                        'tract10_hra': 'HRAs', 'area_type': 'area_type'}
            parcel_output['tract10_hra'] = parcel_output['tract10_hra'].replace({0:'Not HRA',1:'HRAs'})
            parcel_output['tract10_epc'] = parcel_output['tract10_epc'].replace({0:'Not EPC',1:'EPCs'})
        if rtp=="RTP2025":
            area_vars = {'region': "Region", 'tract20_epc': 'CoCs',
                        'tract20_hra': 'HRAs', 'area_type': 'area_type'}
            parcel_output['tract20_hra'] = parcel_output['tract20_hra'].replace({0:'Not HRA',1:'HRAs'})
            parcel_output['tract20_epc'] = parcel_output['tract20_epc'].replace({0:'Not EPC',1:'EPCs'})

        
        # Add constant useful for constistent handling of groupby summaries 
        # across the whole region
        parcel_output['region'] = 'Region'
        
        # main event - loop through combinations of area types and transit service area classifications
        for combo in product(*[area_vars, transit_svcs_cols]):

            # run summary on this combination
            logging.debug(f"Running parcel summaries for {'-'.join(combo)}")
            this_summary_servicelevel = groupby_summaries(
                parcel_output, list(combo)
            )

            # new key, including year - making a new tuple
            this_key = (year,) + combo
            
            # store in the dict
            container[this_key] = this_summary_servicelevel
            logging.debug(f"Summary for {this_key} is {this_summary_servicelevel}")

    # collect the dict of dataframes from each year into a single dataframe
    container_df = pd.concat(
        container,
        names=[
            "year",
            "area_grouping",          # i.e. the area concept
            # i.e.  the classification variable (5-way; 6-way; 2-way)
            "transit_grouping",
            "area_detail",            # i.e. the area concept domain levels
            "service_level_detail",   # i.e. the headway / stop type
        ],
    )
    
    # turn to long format series w multi-index
    container_df = container_df.stack()

    logging.debug(f"head for (wide) {container_df.head()}")

    container_df = (
        container_df.round(3)
        .reset_index(name="value")
        .rename(columns={f"level_{container_df.index.nlevels-1}": "variable"})
    )
    logging.debug(f"head for (long) {container_df.head()}")

    # recode a value so it works readily when concatenating / comparing with the PBA50 performance report version
    transit_service_simplification_map = {
        "Major_Transit_Stop_only": "majorstop"}
    container_df.service_level_detail = container_df.service_level_detail.replace(
        transit_service_simplification_map
    )

    # match the format from  rtp2021 input files to tableau. kind of hacky.
    container_df["name"] = container_df.apply(
        lambda x: f'transitproximity_{x.service_level_detail.lower().replace("_only","")}_shareof_{x["variable"].replace("EMPN", "EMPNjobs")}_{str(x.area_detail)}',
        axis=1,
    )

    container_df["metric"] = "C1"
    container_df["modelrun_alias"] = modelrun_alias
    container_df["modelrun_id"] = modelrun_id
    
    
    # Finally, clean up area values a bit

    container_df['area_grouping'] = container_df.area_grouping.map(area_vars)

    # basic high level summary - regional totals, major stops only
    qry_string_cat5_basic = 'area_grouping=="Region" & transit_grouping.str.contains("cat5$") & service_level_detail=="majorstop"'
    
    # detailed version, without limiting to major stops but all headway categories
    qry_string_cat5_detail = 'transit_grouping.str.contains("cat5$") '
    
    # look through the two query strings and filter the larger frame as appropriate - a basic and a detailed one
    for det, qrystr in {'basic':qry_string_cat5_basic,'detail':qry_string_cat5_detail}.items():

        updated_metrics_tableau_schema = format_for_tableau(
            container_df, qrystring=qrystr
        )

        filename = f"metrics_connected1_transitproximity_{det}.csv"
        filepath = output_path / filename

        updated_metrics_tableau_schema.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
        logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(updated_metrics_tableau_schema), filepath))

    

def format_for_tableau(plan_metrics_df: pd.DataFrame,
                       qrystring: str,
                       ) -> pd.DataFrame:
    """Formats the output for Tableau to fit the format of 
    https://github.com/BayAreaMetro/bayarea_urbansim/blob/main/scripts/proximity2transit.py.

    Args:
        plan_metrics_df (pd.DataFrame): The DataFrame containing transit proximity metrics.
        qrystring (str): A query string to filter the full DataFrame to smaller subsets.

    Returns:
        pd.DataFrame: The formatted DataFrame.
    """
    logging.debug(f"Formatting for Tableau")
    logging.debug(f"Query string: {qrystring}")
    logging.debug(f"Number of metrics: {len(plan_metrics_df)}")

    tableau_column_map = { 'year':'year','modelrun_id':'modelrun_id',
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
        "OTHEMPN":'Jobs',
        "HEREMPN":'Jobs',
        "FPSEMPN":'Jobs',
        "totemp": "Jobs",
        "hhq1": "Households",
        "tothh": "Households",
    }
    
    # tableau friendly names: detail
    tableau_type_subgroup_map = {
        "MWTEMPN": "Manufacturing and Warehousing Jobs",
        "RETEMPN": "Retail Jobs",
        "OTHEMPN":'Government and Information Jobs',
        "HEREMPN":'Health, Education and Recreational Jobs',
        "FPSEMPN":'Financial and Professional Services Jobs',
        
        "totemp": "All Jobs",
        "hhq1": "Households with Low Incomes",
        "tothh": "All Households",
    }

    # first, we subset away most of the categories
    plan_metrics_df_for_tableau = plan_metrics_df.query(qrystring)

    logging.debug(f'After filtering of output for tableau: {plan_metrics_df_for_tableau.head()}')
    
    # then we deal with the schema and domains
    metrics_tableau_schema = (
        plan_metrics_df_for_tableau[
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
    id_cols = ["year","modelrun_id","modelrun_alias", "type_code", "type_group", "type_subgroup"]
    id_extra_cols = [
        "area_grouping",
        "transit_grouping",
        "area_detail",
        "service_level_detail",
    ]
    val_col = ["transitproximity_majorstop_shareof"]

    return metrics_tableau_schema[id_cols + id_extra_cols + val_col]

def format_for_tableau_v2(plan_metrics_df: pd.DataFrame #qrystring: str,
                       ) -> pd.DataFrame:
    """Formats the output for Tableau to fit the format of 
    https://github.com/BayAreaMetro/bayarea_urbansim/blob/main/scripts/proximity2transit.py.

    Args:
        plan_metrics_df (pd.DataFrame): The DataFrame containing transit proximity metrics.
        qrystring (str): A query string to filter the full DataFrame to smaller subsets.

    Returns:
        pd.DataFrame: The formatted DataFrame.
    """
    logging.debug(f"Formatting for Tableau")
    #logging.debug(f"Query string: {qrystring}")
    logging.debug(f"Number of metrics: {len(plan_metrics_df)}")

    tableau_column_map = { 'year':'year','modelrun_id':'modelrun_id',
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
        "OTHEMPN":'Jobs',
        "HEREMPN":'Jobs',
        "FPSEMPN":'Jobs',
        "totemp": "Jobs",
        "hhq1": "Households",
        "tothh": "Households",
    }
    
    # tableau friendly names: detail
    tableau_type_subgroup_map = {
        "MWTEMPN": "Manufacturing and Warehousing Jobs",
        "RETEMPN": "Retail Jobs",
        "OTHEMPN":'Government and Information Jobs',
        "HEREMPN":'Health, Education and Recreational Jobs',
        "FPSEMPN":'Financial and Professional Services Jobs',
        
        "totemp": "All Jobs",
        "hhq1": "Households with Low Incomes",
        "tothh": "All Households",
    }

    # first, we subset away most of the categories
    plan_metrics_df_for_tableau = plan_metrics_df#.query(qrystring)

    logging.debug(f'After filtering of output for tableau: {plan_metrics_df_for_tableau.head()}')
    
    # then we deal with the schema and domains
    metrics_tableau_schema = (
        plan_metrics_df_for_tableau[
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
    id_cols = ["year","modelrun_id","modelrun_alias", "type_code", "type_group", "type_subgroup"]
    id_extra_cols = [
        "area_grouping",
        "transit_grouping",
        "area_detail",
        "service_level_detail",
    ]
    val_col = ["transitproximity_majorstop_shareof"]

    return metrics_tableau_schema[id_cols + id_extra_cols + val_col]


def transit_service_area_share_v2(
                                rtp: str,
                                modelrun_alias: str, 
                                modelrun_id: str, 
                                modelrun_data: dict, 
                                output_path: str,
                                append_output: bool
                                ):

    
    # use later for adding county from fips code
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

    # The transit_scenario_mapping relates the run type to which transit stop buffer universe is appropriate for the tabulation. 
    # e.g. final blueprint simulation runs should be matched with similar scenario transit stops / headways.
    
    # Left hand side is modelrunid_alias - we want to relate that to the specific transit stop map to use
    # where stops and headways will differ for np and dbp. The parcel crosswalk has different classifications
    # for np and dbp.

    transit_scenario_mapping = {
        "NoProject": "np",
        "No Project": "np", # allowing for variations in input here
        "NP":'np',
        "Plus": "dbp",
        "Final Blueprint": "fbp",
        "Draft Blueprint": "dbp",
        "DBP": "dbp", 
        "Alt1": "fbp",
        "Alt2": "fbp",
        'EIR': 'fbp',
        'EIR Alt1': 'fbp',
        'EIR Alt2': 'fbp',
        'EIR Alt2 Var1': 'fbp',
        "Current": "cur", # refers to existing conditions transit stops
    }
    
    # set Boolean to track if a base year run is already processed         
    is_baseyear_processed = False
    
    logging.info(f"Calculating connected for {modelrun_alias} / {modelrun_id}")
    logging.debug(f"Modelrun data years: {modelrun_data.keys()}")
    logging.debug(f"Modelrun data 2050 datasets: {modelrun_data[2050].keys()}")

    # convenience function for easy groupby percentages
    def pct(x):
        return x / x.sum()

    # convenience function for indicator specific groupby summaries, with variable groups
    def groupby_summaries(df, group_vars):

        grp_summary = (
            df.groupby(group_vars).agg(
                agg_mapping
            )
        )
        logging.debug(
            f'Summarizing parcels with respect to: {"; ".join(group_vars)}')
        logging.debug(f"{grp_summary.head()}")

        # this assumed the FIRST group level is the area and not the transit service area one!!
        # We are in other words getting the within-group distribution by transit service area
        # so, for HRAs, what is the share living in major transit areas; same for the region

        grp_summary_shares = (
            grp_summary
            .groupby(level=group_vars[0],
                    group_keys=False)
            .apply(pct)
            .round(3)
        )

        return grp_summary_shares


    # Define columns containing values of interest - more could be added as long as it is present and numeric
    val_cols = ["totemp", "RETEMPN", "MWTEMPN","OTHEMPN","HEREMPN","FPSEMPN", "tothh", "hhq1"]
    
    # use for groupby later to say what do do with each col - we just sum them
    agg_mapping = {col: 'sum' for col in val_cols}

    # get a shorthand alias from the often more verbose one
    #TODO: this should be fixed on the inventory side - using the description field for details
    #modelrun_alias = metrics_utils.classify_runid_alias(modelrun_alias)
    

    SUMMARY_YEARS = sorted(modelrun_data.keys())

    # for storing yearly data
    container = {}


    for year in SUMMARY_YEARS:

        # set boolean for baseyear status
        is_baseyear = int(year) in [2015, 2020, 2023]

        # Process baseyear for No Project only
        if is_baseyear:
            if modelrun_alias == 'No Project':
                logging.info(f'Processing baseyear {year} for No Project.')
            elif modelrun_alias in ['Draft Blueprint', 'Final Blueprint', 'EIR Alt2', 'EIR Alt2 Var1']:
                logging.info(f'Skipping baseyear {year} for {modelrun_alias}.')
                continue
            else:
                logging.info(f'Skipping baseyear {year} for {modelrun_alias}.')
                continue

        # Process 2050 for No Project, Draft Blueprint, and Final Blueprint
        elif year == 2050:
            if modelrun_alias in ['No Project', 'Draft Blueprint', 'Final Blueprint', 
                                  'EIR Alt1', 'EIR Alt2', 'EIR Alt2 Var1']:
                logging.info(f'Processing {year} for {modelrun_alias}.')
            else:
                logging.info(f'Skipping {year} for {modelrun_alias}.')
                continue

        # Skip any other years
        else:
            logging.info(f'Skipping {year} for {modelrun_alias}.')
            continue

        # Continue processing for each year and scenario
        transit_scenario = transit_scenario_mapping.get(modelrun_alias, 'fbp') # Set default to fbp
        parcel_output = modelrun_data[year]["parcel"].copy(deep=True)
        # report shape of parcel_output df
        len_parcels = len(parcel_output)
        logging.debug(f'Cols of parcels {year} in connected func: {parcel_output.columns}')
        logging.debug(f"Parcel output has {len_parcels:,} rows")

        # adding county field from tract ids
        parcel_output['county'] = parcel_output.tract20.map(lambda x: f'{x:011.0f}').str.slice(0,5).map(bayareafips).fillna('MISSING')
        logging.info(f'counts of parcel by county: {parcel_output["county"].value_counts()}')
        
        # Identify the passed scenario-specific columns (fbp no project, current)
        # this returns different classifications for each - like the 5-way or 6-way service level (cat5, cat6)
        # several may be returned depending on how many are in the crosswalk
        # we summarize run data for each classification variable

        # Ensure that we use the current transit universe for 2023 tabulations
        if year == 2023:
            transit_scenario = 'cur'

        transit_svcs_cols = parcel_output.filter(
            regex=transit_scenario
        ).columns.tolist()

        logging.debug(
            f'Transit scenario specific for {transit_scenario=} classifier columns: {"; ".join(transit_svcs_cols)}'
        )

        # Fill missing values and convert specific columns to integers for consistency
        for col in val_cols:
            parcel_output[col] = parcel_output[col].fillna(0).round(0).astype(int)

        # convenience function for easy groupby percentages

        # Now for the summaries - a variable (e.g. tothh) for a geographic area (e.g. region, hra, etc.)
        # is distributed by transit service area (e.g. cat5, cat6, etc.): what share of households
        # live in major transit areas? what share of jobs "live" in major transit areas?


        # we define area_vars to hold different areas of interest to use
        # for the groupby summaries
        if rtp=="RTP2021":
            # RTP2021 was tract10-based
            area_vars = {'region': "Region", 'tract10_epc': 'CoCs',
                         'county':'County',
                        'tract10_hra': 'HRAs', 'area_type': 'area_type'}
            
            parcel_output['tract10_hra'] = parcel_output['tract10_hra'].replace({0:'Not HRA',1:'HRAs'})
            parcel_output['tract10_epc'] = parcel_output['tract10_epc'].replace({0:'Not EPC',1:'EPCs'})
        if rtp=="RTP2025":
            area_vars = {'region': "Region", 'tract20_epc': 'CoCs',
                         'county':'County',
                        'tract20_hra': 'HRAs', 'area_type': 'area_type'}
            
            parcel_output['tract20_hra'] = parcel_output['tract20_hra'].replace({-1:'Not HRA',0:'Not HRA',1:'HRAs'})
            parcel_output['tract20_epc'] = parcel_output['tract20_epc'].replace({-1:'Not EPC',0:'Not EPC',1:'EPCs'})

        parcel_output[transit_svcs_cols] = (parcel_output[transit_svcs_cols]
                                            .fillna(0)
                                            .replace({-1:'Outside Major Transit Buffer',
                                                      0:'Outside Major Transit Buffer',
                                                      1:'Inside Major Transit Buffer'}))
        
        # Add constant useful for constistent handling of groupby summaries 
        # across the whole region - we can use this as an area grouping variable
        # analogous to any other area grouping variable for consistent treatment
        parcel_output['region'] = 'Region'
        
        # main event - loop through combinations of area types and transit service area classifications
        for combo in product(*[area_vars, transit_svcs_cols]):

            # run summary on this combination
            logging.debug(f"Running parcel summaries for {'-'.join(combo)}")
            this_summary_servicelevel = groupby_summaries(
                parcel_output, list(combo)
            )

            # new key, including year - making a new tuple
            this_key = (year,) + combo
            
            # store in the dict
            container[this_key] = this_summary_servicelevel
            logging.debug(f"Summary for {this_key} is {this_summary_servicelevel}")

    
    
    # after the loop we collect the dict of dataframes into a single dataframe
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
    
    # turn to long format series w multi-index
    container_df.columns = container_df.columns.set_names('variable')
    container_df = container_df.stack()

    logging.debug(f"head for (wide) {container_df.head()}")

    container_df = (
        container_df.round(3)
        .reset_index(name="value")
        #.rename(columns={f"level_{container_df.index.nlevels-1}": "variable"})
    )
    logging.debug(f"head for (long) {container_df.head()}")


    container_df["metric"] = "C1"
    container_df["modelrun_alias"] = modelrun_alias
    container_df["modelrun_id"] = modelrun_id
    
    
    # Finally, clean up area values a bit

    container_df['area_grouping'] = container_df.area_grouping.map(area_vars)

    # basic high level summary - regional totals, major stops only
    #qry_string_cat5_basic = 'area_grouping=="Region" & transit_grouping.str.contains("cat5$") & service_level_detail=="majorstop"'
    
    # detailed version, without limiting to major stops but all headway categories
    #qry_string_cat5_detail = 'transit_grouping.str.contains("cat5$") '
    
    # look through the two query strings and filter the larger frame as appropriate - a basic and a detailed one
    #for det, qrystr in {'basic':qry_string_cat5_basic,'detail':qry_string_cat5_detail}.items():


    updated_metrics_tableau_schema = format_for_tableau_v2(
        container_df
    )

    filename = f"metrics_connected1_transitproximity_v2.csv"
    filepath = output_path / filename

    updated_metrics_tableau_schema.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(updated_metrics_tableau_schema), filepath))

