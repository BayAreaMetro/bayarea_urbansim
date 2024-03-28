"""
Script Name: Standalone Land Use Metrics Calculation

Description:
This script dynamically loads data for various Bay Area UrbanSim model runs within specified project directories,
calculates a set of metrics defined in the performance report related to land use, and aggregates these metrics across all
runs into a single CSV file for analysis and review.

Requirements:
- Python 3.6+
- pandas and pathlib, datetime, and logging libraries
- Access to the following folders on the M Drive and Box:
    - M:/urban_modeling/baus/BAUS Inputs
    - M:/urban_modeling/baus/PBA50Plus
    - C:/Users/{user}/Box/Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/PBA50/EIR runs
    - C:/Users/{user}/Box/Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/PBA50/Final Blueprint runs

Usage:
Ensure the base path and crosswalk file paths are correctly set to match the project directory structure.
Run the script in a Python environment where the required libraries are installed.

Functions:
- load_data_for_runs: Loads summary data for each discovered model run.
- load_crosswalk_data: Loads the parcels_geography file to complement the parcels characteristics.
- merge_with_crosswalk: Merges parcel summary dfs with the parcels geography df. 
- extract_ids_from_row:
- calculate_metrics: Calculates metrics for each model run.
- main: Orchestrates the workflow of the script.
"""
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from metrics_utils import load_data_for_runs, load_crosswalk_data, extract_ids_from_row, extract_pba50_concat_values
from metrics_affordable import deed_restricted_affordable_share , new_prod_deed_restricted_affordable_share, at_risk_housing_preserv_share
#from metrics_connected import 
from metrics_diverse import low_income_households_share
#from metrics_healthy import
from metrics_vibrant import jobs_housing_ratio
from metrics_growth import growth_patterns_county, growth_patterns_geography

# Setup logging
current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M") # Generate a timestamp for the log filename
log_filename = f'M:/urban_modeling/baus/PBA50Plus/Metrics/{current_datetime}_log.log' #{run_name}_
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_filename, filemode='w')  # 'w' to overwrite if necessary

# -----------------------------------
# Comprehensive Metrics Calculation
# -----------------------------------
def calculate_metrics(core_summary_dfs, geographic_summary_dfs, modelrun_id, modelrun_alias, plan, output_path):
    """
    Calculates various performance metrics for the Bay Area UrbanSim model runs.

    This function aggregates results from multiple metrics calculations. It utilizes data from core and geographic summaries to compute these metrics 
    and then combines them into a single DataFrame.

    Parameters:
    - core_summary_dfs (list of pd.DataFrame): A list containing two dataframes with core summary data for different years.
    - geographic_summary_dfs (list of pd.DataFrame): A list containing two dataframes with geographic summary data for different years.
    - modelrun_id (str): An identifier for the model run.
    - modelrun_alias (str): The modelrun_alias associated with the model run.
    - plan (str): name of the plan (e.g. pba50 or pba50plus)
    - output_path (str): The file path where the aggregated results DataFrame should be saved as a CSV file.

    Returns:
    - pd.DataFrame: A dataframe containing aggregated results from all metrics calculations, with each metric's results
                    appended as rows.
    """
    logging.info(f"Starting metrics calculation for {modelrun_id} with {modelrun_alias}")
    # Initialize an empty DataFrame for aggregated results
    aggregated_results = pd.DataFrame()

    if len(core_summary_dfs) == 2 and len(geographic_summary_dfs) == 2:
        #dr_share_results = deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        #new_prod_dr_share_results = new_prod_deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        #at_risk_preserv_share_results = at_risk_housing_preserv_share(modelrun_id, modelrun_alias, output_path)
        #li_households_share_results = low_income_households_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        #job_housing_ratio_results = jobs_housing_ratio(geographic_summary_dfs[0], geographic_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        growth_patterns_county_results = growth_patterns_county(geographic_summary_dfs[0], geographic_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        growth_patterns_geography_results = growth_patterns_geography(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)

        # Assign metric_type for distinction
        #dr_share_results ['metric_type'] = 'affordable'
        #new_prod_dr_share_results['metric_type'] = 'affordable_new_prod'
        #at_risk_preserv_share_results['metric_type'] = 'affordable_at_risk_preserv'
        #li_households_share_results['metric_type'] = 'diverse'
        #job_housing_ratio_results['metric_type'] = 'vibrant'
        growth_patterns_county_results['metric_type'] = 'growth_county'
        growth_patterns_geography_results['metric_type'] = 'growth_geography'

        # Combine the results
        aggregated_results = pd.concat([growth_patterns_county_results, growth_patterns_geography_results], ignore_index=True)
        #  dr_share_results, new_prod_dr_share_results, at_risk_preserv_share_results, li_households_share_results, job_housing_ratio_results, 

        if not aggregated_results.empty:
            logging.info(f"Aggregated results contain {aggregated_results.shape[0]} rows. Proceeding to save to CSV.")
        else:
            logging.error("Aggregated results are empty. Check earlier steps for issues.")

    return aggregated_results

def main():
    # Define paths
    output_path = Path("M:/urban_modeling/baus/PBA50Plus")
    path_dictionary = "M:/urban_modeling/baus/PBA50Plus/Metrics/PBA50Plus_model_run_inventory.csv" #or M:/urban_modeling/baus/PBA50Plus/Metrics/PBA50plus_model_run_inventory.csv
    
    # Load the model runs inventory
    df_model_runs = pd.read_csv(path_dictionary, low_memory=False)

    # Define crosswalk paths for both plans
    crosswalk_paths = {"pba50plus": "M:/urban_modeling/baus/BAUS Inputs/basis_inputs/crosswalks/parcels_geography_2024_02_14.csv",
                       "pba50": {"geography": "C:/Users/nrezaei/Box/Horizon and Plan Bay Area 2050/Equity and Performance/7_Analysis/Metrics/metrics_input_files/COCs_ACS2018_tbl_TEMP.csv",
                                 "tract": "C:/Users/nrezaei/Box/Horizon and Plan Bay Area 2050/Equity and Performance/7_Analysis/Metrics/metrics_input_files/parcel_tract_crosswalk.csv"}}
    
    # Iterate over each row in the csv file to get the directory paths
    for _, row in df_model_runs.iterrows():
        directory_path = Path(row['directory'])
        plan = row['plan']
    
        # Choose the correct crosswalk path based on the plan
        crosswalk_path = crosswalk_paths.get(plan.lower(), None)
        if not crosswalk_path:
            logging.error(f"Unrecognized plan or missing crosswalk path for {plan}. Skipping this model run.")
            continue 

        logging.info(f"Processing model run: {directory_path}")
        logging.info("Loading crosswalk data...")
        
        if plan == "pba50plus":
            parcels_geography = load_crosswalk_data(crosswalk_paths[plan])
        elif plan == "pba50":
            geography_crosswalk = load_crosswalk_data(crosswalk_paths[plan]['geography'])
            tract_crosswalk = load_crosswalk_data(crosswalk_paths[plan]['tract'])
        
        core_summaries_path = directory_path / ("core_summaries" if plan == "pba50plus" else "")
        geographic_summaries_path = directory_path / ("geographic_summaries" if plan == "pba50plus" else "")
        
        # Load data for the current run
        core_dfs, geo_dfs = load_data_for_runs([core_summaries_path], [geographic_summaries_path], plan)
        
        if plan == "pba50":
            for i, df in enumerate(core_dfs):
                column_name = None
                if 'fbpchcat' in df.columns:
                    column_name = 'fbpchcat'
                elif 'eirzoningmodcat' in df.columns:
                    column_name = 'eirzoningmodcat'
                if column_name:
                    # Directly expand the results of extract_pba50_concat_values into new columns in the DataFrame
                    new_columns = df[column_name].apply(extract_pba50_concat_values).apply(pd.Series)
                    new_columns.columns = ['gg_id', 'tra_id', 'hra_id', 'dis_id']
                    df = df.join(new_columns)
                    logging.info("Sample data after joining new columns:")
                    logging.info(df.head().to_dict(orient='records')[0])
                    logging.info(f"First few 'tra_id' values after extraction: {df['tra_id'].head().to_list()}")
                else:
                    logging.warning(f"'Neither 'fbpchcat' nor 'eirzoningmodcat' found in DataFrame for model run {modelrun_id}. Skipping this DataFrame.")

                # Merging using the tract and geography crosswalks
                df_merged_with_tract = df.merge(tract_crosswalk, on="parcel_id", how="left")
                logging.info("DataFrame after first merge with tract crosswalk:")
                logging.info(df_merged_with_tract.head().to_dict(orient='records')[0])

                df_final = df_merged_with_tract.merge(geography_crosswalk, on="tract_id", how="left")
                logging.info("DataFrame after second merge with geography crosswalk:")
                logging.info(df_final.head().to_dict(orient='records')[0])

                # Retain only a subset of columns after merging
                columns_to_keep = ['parcel_id', 'tract_id', column_name, 'gg_id', 'tra_id', 'hra_id', 'dis_id', 'hhq1', 'hhq2', 'hhq3', 'hhq4', 'tothh', 'totemp', 'deed_restricted_units', 'residential_units', 'preserved_units', 'geoid', 'tot_pop', 'coc_flag_pba2050', 'coc_class']
                df_final = df_final[columns_to_keep]
                
                core_dfs[i] = df_final
                logging.info(f"New columns added: {new_columns.columns.tolist()}")

        elif plan == "pba50plus":
            # For "pba50plus", merge the loaded data with the crosswalk data
            core_dfs = [df.merge(parcels_geography, left_on="parcel_id", right_on="PARCEL_ID", how="left") for df in core_dfs]

        # Extract 'modelrun_id' and 'modelrun_alias' for the current model run directory
        modelrun_id, modelrun_alias = extract_ids_from_row(row)
        logging.info(f"Loaded {len(core_dfs)} core dataframes and {len(geo_dfs)} geographic dataframes for {modelrun_id}")

        # Calculate metrics for the current run
        run_metrics = calculate_metrics(core_dfs, geo_dfs, modelrun_alias, modelrun_id, plan, output_path)
        logging.info(f"Calculating metrics for {modelrun_id}. Core summary DFs: {len(core_dfs)}, Geographic summary DFs: {len(geo_dfs)}")

        # Save affordable metrics
        affordable_metrics = run_metrics[run_metrics['metric_type'] == 'affordable'].drop_duplicates(subset=['modelrun_id', 'modelrun_alias', 'area', 'deed_restricted_pct'])
        if not affordable_metrics.empty:
            filename = f"metrics_affordable2_{plan}_deed_restricted_pct_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                affordable_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_alias', 'area', 'deed_restricted_pct'], mode='a', header=False, index=False)
            else:
                affordable_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_alias', 'area', 'deed_restricted_pct'], mode='w', header=True, index=False)
            logging.info(f"Saved affordable metric results to {filepath}")

        # Save new production deed-restricted affordable metrics
        affordable_new_prod_metrics = run_metrics[run_metrics['metric_type'] == 'affordable_new_prod'].drop_duplicates(subset=['modelrun_id', 'modelrun_alias', 'area', 'deed_restricted_pct_newUnits'])
        if not affordable_new_prod_metrics.empty:
            filename = f"metrics_affordable2_{plan}_newUnits_deed_restricted_pct_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                affordable_new_prod_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_alias', 'area', 'deed_restricted_pct_newUnits'], mode='a', header=False, index=False)
            else:
                affordable_new_prod_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_alias', 'area', 'deed_restricted_pct_newUnits'], mode='w', header=True, index=False)
            logging.info(f"Saved new production affordable metric results to {filepath}")

        # Save at risk preserved metric 
        affordable_at_risk_preserved_metrics = run_metrics[run_metrics['metric_type'] == 'affordable_at_risk_preserv'].drop_duplicates(subset=['modelrun_id', 'modelrun_alias', 'area_alias', 'at_risk_preserv_pct'])
        if not affordable_at_risk_preserved_metrics.empty:
            filename = f"metrics_affordable2_{plan}_at_risk_housing_preserve_pct_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                affordable_at_risk_preserved_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_alias', 'at_risk_preserv_pct'], mode='a', header=False, index=False)
            else:
                affordable_at_risk_preserved_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_alias', 'at_risk_preserv_pct'], mode='w', header=True, index=False)
            logging.info(f"Saved presereved affordable metric results to {filepath}")

        # Save vibrant metrics
        vibrant_metrics = run_metrics[run_metrics['metric_type'] == 'vibrant']
        if not vibrant_metrics.empty:
            filename = f"metrics_vibrant1_{plan}_jobs_housing_ratio_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                vibrant_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'county', 'jobs_housing_ratio'], mode='a', header=False, index=False)
            else:
                vibrant_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'county', 'jobs_housing_ratio'], mode='w', header=True, index=False)
            logging.info(f"Saved vibrant metric results to {filepath}")

        # Save diverse metrics
        diverse_metrics = run_metrics[run_metrics['metric_type'] == 'diverse']
        if not diverse_metrics.empty:
            filename = f"metrics_diverse1_{plan}_low_income_households_share_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                diverse_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area', 'Q1HH_share'], mode='a', header=False, index=False)
            else:
                diverse_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area', 'Q1HH_share'], mode='w', header=True, index=False)
            logging.info(f"Saved diverse metric results to {filepath}")

        # Save growth metrics
        growth_metrics = run_metrics[run_metrics['metric_type'] == 'growth_county']
        if not growth_metrics.empty:
            filename = f"metrics_growthPattern_county_{plan}_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                growth_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'county', 'TotHH', 'TotJobs', 'hh_share_of_growth', 'jobs_share_of_growth'], mode='a', header=False, index=False)
            else:
                growth_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'county', 'TotHH', 'TotJobs', 'hh_share_of_growth', 'jobs_share_of_growth'], mode='w', header=True, index=False)
            logging.info(f"Saved growth metric results to {filepath}")

        growth_metrics = run_metrics[run_metrics['metric_type'] == 'growth_geography']
        if not growth_metrics.empty:
            filename = f"metrics_growthPattern_geography_{plan}_{datetime.now().strftime('%Y_%m_%d')}.csv"
            filepath = output_path / "Metrics" / filename
            if filepath.is_file():
                growth_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_name', 'area_alias', 'TotHH', 'TotJobs', 'hh_share_of_growth', 'jobs_share_of_growth'], mode='a', header=False, index=False)
            else:
                growth_metrics.to_csv(filepath, columns=['modelrun_id', 'modelrun_alias', 'area_name', 'area_alias', 'TotHH', 'TotJobs', 'hh_share_of_growth', 'jobs_share_of_growth'], mode='w', header=True, index=False)
            logging.info(f"Saved growth metric for geographies results to {filepath}")

if __name__ == "__main__":
    main()