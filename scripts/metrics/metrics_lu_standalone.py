USAGE="""
This script loads data for various Bay Area UrbanSim model runs and calculates
a set of metrics defined in the performance report related to land use

Requirements:
- Python 3.6+ and pandas

INPUT:
- A model run inventory for the given rtp
  RTP2021: Box/Horizon and Plan Bay Area 2050/Equity and Performance/7_Analysis/Metrics/metrics_input_files/run_inventory.csv
  RTP2025: M:/urban_modeling/baus/PBA50Plus/PBA50Plus_model_run_inventory.csv
- Crosswalk files:

- For each model run in the inventory file:
  - parcels summary files
  - county summary files
OUTPUT:

Functions:
- load_data_for_runs: Loads summary data for each discovered model run.
- load_crosswalk_data: Loads the parcels_geography file to complement the parcels characteristics.
- extract_ids_from_row:
- calculate_metrics: Calculates metrics for each model run.
- main: Orchestrates the workflow of the script.
"""
import argparse, datetime, logging, os, pathlib, sys
import pandas as pd
import metrics_utils
import metrics_growth
from metrics_affordable import deed_restricted_affordable_share , new_prod_deed_restricted_affordable_share, at_risk_housing_preserv_share
#from metrics_connected import 
from metrics_diverse import low_income_households_share
#from metrics_healthy import
from metrics_vibrant import jobs_housing_ratio


def main():
    pd.options.display.width = 500 # this goes to log file
    pd.options.display.max_columns = 100
    pd.options.display.max_rows = 500

    parser = argparse.ArgumentParser(
        description = USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('rtp', type=str, choices=['RTP2021','RTP2025'])
    parser.add_argument('--test', action='store_true', help='If passed, writes output to cwd instead of METRICS_OUTPUT_DIR')
    args = parser.parse_args()

    # RTP2025 / PBA50+ settings
    USERNAME = os.environ.get('USERNAME')
    if USERNAME.lower() in ['lzorn']:
        BOX_DIR = pathlib.Path("E:/Box")
    else:
        BOX_DIR = pathlib.Path(f"C:/Users/{USERNAME}/Box")
    MODEL_RUNS_DIR     = pathlib.Path("M:/urban_modeling/baus/PBA50Plus/")
    METRICS_DIR        = MODEL_RUNS_DIR / "Metrics"
    RUN_INVENTORY_FILE = METRICS_DIR / "PBA50Plus_model_run_inventory.csv"
    OUTPUT_PATH        = BOX_DIR / "Plan Bay Area 2050+/Performance and Equity/Plan Performance/Equity_Performance_Metrics/Draft_Blueprint"
    LOG_FILENAME       = "metrics_lu_standalone_{}.log" # loglevel

    # this is for QAQC
    if args.rtp == "RTP2021":
        METRICS_DIR        = BOX_DIR / "Horizon and Plan Bay Area 2050/Equity and Performance/7_Analysis/Metrics"
        RUN_INVENTORY_FILE = METRICS_DIR / "metrics_input_files/run_inventory.csv"
        MODEL_RUNS_DIR     = BOX_DIR / "Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/PBA50"
        OUTPUT_PATH        = BOX_DIR / "Plan Bay Area 2050+/Performance and Equity/Plan Performance/Equity_Performance_Metrics/PBA50_reproduce_for_QA"

    # Setup logging
    if args.test:
        OUTPUT_PATH = pathlib.Path(".")  # current directory

    ################## create logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ################## console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(ch)
    ################## file handler - info
    fh = logging.FileHandler(OUTPUT_PATH / LOG_FILENAME.format("info"), mode='w')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh)
    ################## file handler - debug
    fh = logging.FileHandler(OUTPUT_PATH / LOG_FILENAME.format("debug"), mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh)

    logging.info("Running metrics_lu_standalone.py with args {}".format(args))
    logging.info(f"    MODEL_RUNS_DIR: {MODEL_RUNS_DIR}")
    logging.info(f"       METRICS_DIR: {METRICS_DIR}")
    logging.info(f"RUN_INVENTORY_FILE: {RUN_INVENTORY_FILE}")
    logging.info(f"       OUTPUT_PATH: {OUTPUT_PATH}")

    # Load the model runs inventory
    # expect columns "directory" and "scenario_group"
    model_runs_df = pd.read_csv(RUN_INVENTORY_FILE, low_memory=False)
    logging.info("model_runs_df:\n{}".format(model_runs_df))

    # Iterate over each model run
    append_output = False # First model run output - don't append output
    for row in model_runs_df.to_dict('records'):

        # directory is relative to MODEL_RUNS_DIR
        run_directory_path = MODEL_RUNS_DIR / row['directory']
        modelrun_alias = row['alias']
        modelrun_id = run_directory_path.parts[-1]

        logging.info(f"Processing run modelrun_alias:[{modelrun_alias}] modelrun_id:[{modelrun_id}] run_directory_path:{run_directory_path}")
        
        # Load data for the current run
        modelrun_data = metrics_utils.load_data_for_runs(args.rtp, METRICS_DIR, run_directory_path)
        
        # Calculate metrics for the current run

        #dr_share_results = deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        #new_prod_dr_share_results = new_prod_deed_restricted_affordable_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        #at_risk_preserv_share_results = at_risk_housing_preserv_share(modelrun_id, modelrun_alias, output_path)
        #li_households_share_results = low_income_households_share(core_summary_dfs[0], core_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        #job_housing_ratio_results = jobs_housing_ratio(geographic_summary_dfs[0], geographic_summary_dfs[1], modelrun_id, modelrun_alias, plan, output_path)
        metrics_growth.growth_patterns_county(
            args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)
        metrics_growth.growth_patterns_geography(
            args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)

        append_output = True
        continue
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