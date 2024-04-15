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
import argparse, datetime, logging, os, pathlib, sys, getpass
import pandas as pd
import metrics_utils

import metrics_affordable
import metrics_connected
import metrics_diverse
import metrics_growth
import metrics_healthy
import metrics_vibrant

def main():
    pd.options.display.width = 500 # this goes to log file
    pd.options.display.max_columns = 100
    pd.options.display.max_rows = 500

    parser = argparse.ArgumentParser(
        description = USAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('rtp', type=str, choices=['RTP2021','RTP2025'])
    parser.add_argument('--test', action='store_true', help='If passed, writes output to cwd instead of METRICS_OUTPUT_DIR')
    parser.add_argument('--only', required=False, choices=['affordable','connected','diverse','growth','healthy','vibrant'], 
                        help='To only run one metric set')

    args = parser.parse_args()

    # RTP2025 / PBA50+ settings
    USERNAME = getpass.getuser()
    HOME_DIR = pathlib.Path.home()
    
    # set the path for M: drive
    # from OSX, M:/ may be mounted to /Volumes/Data/Models
    M_DRIVE = "/Volumes/Data/Models" if os.name != "nt" else "M:/"


    if USERNAME.lower() in ['lzorn']:
        BOX_DIR = pathlib.Path("E:/Box")
    else:
        BOX_DIR = HOME_DIR / 'Box'
    
    MODEL_RUNS_DIR     = pathlib.Path(M_DRIVE, "urban_modeling/baus/PBA50Plus/")
    RUN_INVENTORY_FILE = MODEL_RUNS_DIR / "Metrics/PBA50Plus_model_run_inventory.csv"
    OUTPUT_PATH        = BOX_DIR / "Plan Bay Area 2050+/Performance and Equity/Plan Performance/Equity_Performance_Metrics/Draft_Blueprint"
    METRICS_DIR        = OUTPUT_PATH
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
        modelrun_id = row['directory']

        logging.info(f"Processing run modelrun_alias:[{modelrun_alias}] modelrun_id:[{modelrun_id}] run_directory_path:{run_directory_path}")
        
        # Load data for the current run
        modelrun_data = metrics_utils.load_data_for_runs(args.rtp, METRICS_DIR, run_directory_path, modelrun_alias)
        SUMMARY_YEARS = sorted(modelrun_data.keys())

        if (args.only == None) or (args.only == 'affordable'):
            metrics_affordable.deed_restricted_affordable_share(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)
    
            metrics_affordable.at_risk_housing_preserve_share(
                SUMMARY_YEARS[-1], modelrun_alias, modelrun_id, OUTPUT_PATH, append_output)
            
        if (args.only == None) or (args.only == 'diverse'):
            metrics_diverse.gentrify_displacement_tracts(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)

            metrics_diverse.low_income_households_share(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)
            
        if (args.only == None) or (args.only == 'growth'):
            metrics_growth.growth_patterns_county(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)
            metrics_growth.growth_patterns_geography(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)

        if (args.only == None) or (args.only == 'vibrant'):
            metrics_vibrant.jobs_housing_ratio(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)
            
        
        if (args.only == None) or (args.only == 'connected'):
            metrics_connected.transit_service_area_share(
                args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)



        if (args.only == None) or (args.only == 'healthy'):
            metrics_healthy.urban_park_acres(
                BOX_DIR, args.rtp, modelrun_alias, modelrun_id, modelrun_data, OUTPUT_PATH, append_output)

        # output files are started; append henceforth
        append_output = True

if __name__ == "__main__":
    main()