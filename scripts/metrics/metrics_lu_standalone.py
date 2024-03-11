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
from metrics_utils import load_data_for_runs, load_crosswalk_data, extract_ids_from_row
from metrics_affordable import deed_restricted_affordable_share, new_prod_deed_restricted_affordable_share
# from metrics_connected import 
from  metrics_diverse import low_income_households_share
# from metrics_healthy import
from metrics_vibrant import jobs_housing_ratio
import glob

# Setup logging
current_datetime = datetime.now().strftime("%Y-%m-%d_%H%M") # Generate a timestamp for the log filename
log_filename = f'M:/urban_modeling/baus/PBA50Plus/Metrics/{current_datetime}_log.log' #{run_name}_
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_filename, filemode='w')  # 'w' to overwrite if necessary

# -----------------------------------
# Comprehensive Metrics Calculation
# -----------------------------------
def calculate_metrics(core_summary_dfs, geographic_summary_dfs, modelrun_id, model_run_alias, plan, output_path):
    """
    Calculates various performance metrics for the Bay Area UrbanSim model runs.

    This function aggregates results from multiple metrics calculations. It utilizes data from core and geographic summaries to compute these metrics 
    and then combines them into a single DataFrame.

    Parameters:
    - core_summary_dfs (list of pd.DataFrame): A list containing two dataframes with core summary data for different years.
    - geographic_summary_dfs (list of pd.DataFrame): A list containing two dataframes with geographic summary data for different years.
    - modelrun_id (str): An identifier for the model run.
    - model_run_alias (str): The model_run_alias associated with the model run.
    - plan (str): name of the plan (e.g. pba50 or pba50plus)
    - output_path (str): The file path where the aggregated results DataFrame should be saved as a CSV file.

    Returns:
    - pd.DataFrame: A dataframe containing aggregated results from all metrics calculations, with each metric's results
                    appended as rows.
    """
    logging.info(f"Starting metrics calculation for {modelrun_id} with {model_run_alias}")
    # Initialize an empty DataFrame for aggregated results
    aggregated_results = pd.DataFrame()

    if len(core_summary_dfs) == 2 and len(geographic_summary_dfs) == 2:
        #dr_share_results = deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, model_run_alias)
        #new_prod_dr_share_results = new_prod_deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, model_run_alias)
        #li_households_share_results = low_income_households_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, model_run_alias, pba50plus_path)
        job_housing_ratio_results = jobs_housing_ratio(geographic_summary_dfs[0], geographic_summary_dfs[1], modelrun_id, model_run_alias, plan, output_path)

        # Aggregate the results
        all_results = pd.concat([job_housing_ratio_results], ignore_index=True) #dr_share_results, new_prod_dr_share_results, li_households_share_results, 

        aggregated_results = pd.concat([aggregated_results, all_results], ignore_index=True)

        if not aggregated_results.empty:
            logging.info(f"Aggregated results contain {aggregated_results.shape[0]} rows. Proceeding to save to CSV.")
        else:
            logging.error("Aggregated results are empty. Check earlier steps for issues.")

    return aggregated_results

def main():
    # Define paths
    output_path = Path("M:/urban_modeling/baus/PBA50Plus")
    path_dictionary = "M:/urban_modeling/baus/PBA50Plus/Metrics/PBA50plus_model_run_inventory.csv" #or M:/urban_modeling/baus/PBA50Plus/Metrics/PBA50_model_run_inventory.csv
    
    # Load the model runs inventory
    df_model_runs = pd.read_csv(path_dictionary, low_memory=False)

    # Define crosswalk paths for both plans
    crosswalk_paths = {"pba50plus": "M:/urban_modeling/baus/BAUS Inputs/basis_inputs/crosswalks/parcels_geography_2024_02_14.csv", 
                       "pba50": "C:/Users/nrezaei/Box/Horizon and Plan Bay Area 2050/Equity and Performance/7_Analysis/Metrics/metrics_input_files/2021_02_25_parcels_geography.csv"}

    # Initialize an empty DataFrame to collect all metrics
    all_metrics = pd.DataFrame()
    
    #vibrant_results = []
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
        
        parcels_geography = load_crosswalk_data(crosswalk_path)

        if plan == "pba50":
            core_summaries_path = directory_path 
            geographic_summaries_path = directory_path
        elif plan == "pba50plus":
            core_summaries_path = directory_path / "core_summaries"
            geographic_summaries_path = directory_path / "geographic_summaries"
        else:
            logging.error(f"Unrecognized plan: {plan}")
            continue
        
        # Load data for the current run
        core_dfs, geo_dfs = load_data_for_runs([core_summaries_path], [geographic_summaries_path], plan)

        # Merge the loaded data with the crosswalk data
        core_dfs_merged = [df.merge(parcels_geography, left_on="parcel_id", right_on="PARCEL_ID", how="left") for df in core_dfs]

        # Extract 'modelrun_id' and 'model_run_alias' for the current model run directory
        modelrun_id, model_run_alias = extract_ids_from_row(row)
        logging.info(f"Loaded {len(core_dfs)} core dataframes and {len(geo_dfs)} geographic dataframes for {modelrun_id}")

        # Calculate metrics for the current run
        run_metrics = calculate_metrics(core_dfs_merged, geo_dfs, model_run_alias, modelrun_id, plan, output_path)
        logging.info(f"Calculating metrics for {modelrun_id}. Core summary DFs: {len(core_dfs)}, Geographic summary DFs: {len(geo_dfs)}")

        # Collect the metrics from all runs
        all_metrics = pd.concat([all_metrics, run_metrics], ignore_index=True)

        # After all runs, concatenate and save vibrant metrics
        filename = f"metrics_vibrant1_{plan}_jobs_housing_ratio_{datetime.now().strftime('%Y-%m-%d_%H')}.csv"
        filepath = Path(output_path) / "Metrics" / filename
        all_metrics.to_csv(filepath, index=False)
        logging.info(f"Saved all vibrant metric results to {filepath}")

if __name__ == "__main__":
    main()