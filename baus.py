from __future__ import print_function
import os
import logging
import pathlib
import sys
import time
import traceback
from baus import (
    datasources, variables, models, subsidies, ual, slr, earthquake, 
    utils, preprocessing)
from baus.tests import validation

from baus.summaries import (
    core_summaries, geographic_summaries, affordable_housing_summaries, 
    hazards_summaries, metrics, travel_model_summaries)

from baus.visualizer import push_model_files
import baus.slack
import baus.debug
import logging_setup

import numpy as np
import pandas as pd
import socket
import argparse
import urbansim
import urbansim_defaults
import orca
import orca_test
import pandana
import shutil
import logging

        
# Get repo deets        
CURRENT_BRANCH = os.popen('git rev-parse --abbrev-ref HEAD').read().rstrip()
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read().rstrip()

# Configure argument parsing
parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument('--run_setup_yaml', default='run_setup.yaml', help='Specify run_setup.yaml file to use')
parser.add_argument('--mode', action='store', dest='mode', default='simulation', help='which mode to run (see code for mode options)')
parser.add_argument('-i', action='store_true', dest='interactive', default=False, help='enter interactive mode after imports')
parser.add_argument('--set-random-seed', action='store_true', dest='set_random_seed', default=False, help='set a random seed for consistent stochastic output')
parser.add_argument('--disable-slack', action='store_true', dest='no_slack', default=False, help='disable slack outputs')
parser.add_argument('--enable-asana', action='store_true', dest='use_asana', default=False, help='disable Asana task creation')

options = parser.parse_args()

# Harvest constants - 
INTERACT = options.interactive

# use the given run_setup.yaml file
orca.add_injectable("run_setup_yaml", options.run_setup_yaml)

MODE = options.mode

# Flip the boolean since it is a disable flag
SLACK = ~options.no_slack

# Get a few orca objects
run_setup = orca.get_injectable("run_setup")
run_name = orca.get_injectable("run_name")

# Prepare output dir for run
outputs_dir = pathlib.Path(orca.get_injectable("outputs_dir"))
outputs_dir.mkdir(parents=True, exist_ok=True)

BASE_YEAR = run_setup["base_year"]
FINAL_YEAR = run_setup["final_year"]
# stop the simulation early for testing/debuggin
STOP_YEAR = run_setup["stop_year"] if "stop_year" in run_setup else FINAL_YEAR
EVERY_NTH_YEAR = 5

orca.add_injectable("base_year", BASE_YEAR)
orca.add_injectable("final_year", FINAL_YEAR)
orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)

if SLACK:

    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    slack_token = os.environ.get("SLACK_TOKEN")
    
    if slack_token is None:
        raise EnvironmentError("SLACK logging was requested but SLACK_TOKEN environment variable is not set. Please set it to enable Slack integration.")

    host = socket.gethostname()
    client = WebClient(token=slack_token)
    slack_channel = "#urbansim_sim_update"
    orca.add_injectable('slack_client',client)
    orca.add_injectable('slack_channel',slack_channel)

if options.set_random_seed:
    SET_RANDOM_SEED = True
    np.random.seed(42)
else:
    SET_RANDOM_SEED = False

if options.use_asana:
    ASANA = True
    
    from scripts.meta.asana_utils import (
    create_asana_task_from_yaml,
    add_comment_to_task, 
    mark_task_as_complete)
    # hard code this for now
    ASANA_SECTION_NAME = 'Final Blueprint Runs'
else:
    ASANA = False


# Set up BAUS logging to write to the specified log file
# Get the outputs directory and run name
log_file_path = os.path.join(orca.get_injectable("outputs_dir"), f"{run_name}.log")
print("Writing to log {}".format(log_file_path))
try:
    logger = logging_setup.setup_logging(log_file_path, logging.DEBUG)
    print("logger={}".format(logger))
except Exception as inst:
    print("Exception occured setting up logging")
    print(inst)
    sys.exit()

orca.add_injectable('logger',logger)
logger.info("***The standard stream is being written to the log file***")
logger.info("Started: %s", time.ctime())
logger.info("Current Branch: %s", CURRENT_BRANCH)
logger.info("Current Commit: %s", CURRENT_COMMIT)
logger.info("Set Random Seed: %s", SET_RANDOM_SEED)
logger.info("Python version: %s", sys.version.split('|')[0])
logger.info("UrbanSim version: %s", urbansim.__version__)
logger.info("UrbanSim Defaults version: %s", urbansim_defaults.__version__)
logger.info("Orca version: %s", orca.__version__)
logger.info("Orca Test version: %s", orca_test.__version__)
logger.info("Pandana version: %s", pandana.__version__)
logger.info("Numpy version: %s", np.__version__)
logger.info("Pandas version: %s", pd.__version__)

logger.info("SLACK: %s", SLACK)
logger.info("MODE: %s", MODE)


def run_models(mode):

    if mode == "estimation":

        orca.run([
            "neighborhood_vars",         
            "regional_vars",             
            "rsh_estimate",             
            "nrh_estimate",         
            "rsh_simulate",
            "nrh_simulate",
            "hlcm_estimate",           
            "elcm_estimate",         
        ])


    elif mode == "preprocessing":

        orca.run([
            "preproc_jobs",
            "preproc_households",
            "preproc_buildings",
            "initialize_residential_units"
        ])


    elif mode == "simulation":

        baseyear_models = [
        
            "slr_inundate",
            "slr_remove_dev",
            "eq_code_buildings",
            "earthquake_demolish",

            "neighborhood_vars",  
            "regional_vars",  

            "rsh_simulate",   
            "rrh_simulate", 
            "nrh_simulate",
            "assign_tenure_to_new_units",

            "household_relocation",
            "households_transition",

            "reconcile_unplaced_households",
            "jobs_transition",

            "hlcm_owner_lowincome_simulate",
            "hlcm_renter_lowincome_simulate",

            "hlcm_owner_simulate",
            "hlcm_renter_simulate",

            "hlcm_owner_simulate_no_unplaced",
            "hlcm_owner_lowincome_simulate_no_unplaced",
            "hlcm_renter_simulate_no_unplaced",
            "hlcm_renter_lowincome_simulate_no_unplaced",

            "reconcile_placed_households",

            "elcm_simulate",

            "price_vars"]

        if not run_setup["run_slr"]:
            baseyear_models.remove("slr_inundate")
            baseyear_models.remove("slr_remove_dev")

        if not run_setup["run_eq"]:
            baseyear_models.remove("eq_code_buildings")
            baseyear_models.remove("earthquake_demolish")
    
        baseyear_summary_models = [
            "simulation_validation",

            "disaggregate_output",

            "hazards_slr_summary",
            "hazards_eq_summary",

            "deed_restricted_units_summary",

            "geographic_summary",

            "taz1_summary",
            "maz_marginals",
            "maz_summary",
            "taz2_marginals",
            "county_marginals",
            "region_marginals",
        ]
            
        baseyear_metrics_models = [
            "growth_geography_metrics",
            "deed_restricted_units_metrics",
            "household_income_metrics",
            "equity_metrics",
            "jobs_housing_metrics",
            "jobs_metrics",
            "slr_metrics",
            "earthquake_metrics",
            "greenfield_metrics",
        ]
            
        simulation_models = [
            "debug",
            "slr_inundate",
            "slr_remove_dev",
            "eq_code_buildings",
            "earthquake_demolish",

            "neighborhood_vars",   
            "regional_vars",      

            "nrh_simulate",

            "household_relocation",
            "households_transition",

            "reconcile_unplaced_households",

            "jobs_relocation",
            "jobs_transition",

            "balance_rental_and_ownership_hedonics",

            "price_vars",
            "scheduled_development_events",

            "preserve_affordable",

            "lump_sum_accounts",
            "subsidized_residential_developer_lump_sum_accts",


            "office_lump_sum_accounts",
            "subsidized_office_developer_lump_sum_accts",

            "alt_feasibility",
            "subsidized_residential_feasibility",
            "subsidized_residential_developer_vmt",
        #    "subsidized_residential_feasibility",
        #    "subsidized_residential_developer_jobs_housing",

            "residential_developer",
            "developer_reprocess",
            "retail_developer",

            "office_developer",
            "subsidized_office_developer_vmt",

            "accessory_units_strategy",
            "calculate_vmt_fees",

            "remove_old_units",
            "initialize_new_units",
            "reconcile_unplaced_households",

            "rsh_simulate",   
            "rrh_simulate", 

            "assign_tenure_to_new_units",

            "hlcm_owner_lowincome_simulate",
            "hlcm_renter_lowincome_simulate",

            # the hlcms above could be moved above the developer again, 
            # but we would have to run the hedonics and assign tenure to units twice
            "hlcm_owner_simulate",
            "hlcm_renter_simulate",
            "hlcm_owner_simulate_no_unplaced",
            "hlcm_owner_lowincome_simulate_no_unplaced",
            "hlcm_renter_simulate_no_unplaced",
            "hlcm_renter_lowincome_simulate_no_unplaced",

            "reconcile_placed_households",

            "proportional_elcm",
            "gov_transit_elcm",
            "elcm_simulate_ec5",
            "elcm_simulate",  

            "calculate_vmt_fees",
            "calculate_jobs_housing_fees",
        ]
        if not run_setup["run_jobs_to_transit_strategy_elcm"]:
            simulation_models.remove("elcm_simulate_ec5")
            print('Removing `elcm_simulate_ec5`')

        if not run_setup["run_jobs_to_transit_strategy_random"]:
            simulation_models.remove("gov_transit_elcm")
            print('Removing `gov_transit_elcm`')
        
        if not run_setup["run_slr"]:
            simulation_models.remove("slr_inundate")
            simulation_models.remove("slr_remove_dev")

        if not run_setup["run_eq"]:
            simulation_models.remove("eq_code_buildings")
            simulation_models.remove("earthquake_demolish")

        if not run_setup["run_housing_preservation_strategy"]:
            simulation_models.remove("preserve_affordable")

        if not run_setup["run_office_bond_strategy"]:
            simulation_models.remove("office_lump_sum_accounts")
            simulation_models.remove("subsidized_office_developer_lump_sum_accts")

        if not run_setup["run_adu_strategy"]:
            simulation_models.remove("accessory_units_strategy")

        if not run_setup["run_vmt_fee_com_for_com_strategy"]:
            simulation_models.remove("calculate_vmt_fees")
            simulation_models.remove("subsidized_office_developer_vmt")
        if not run_setup["run_vmt_fee_com_for_res_strategy"] or run_setup["run_vmt_fee_res_for_res_strategy"]:
            simulation_models.remove("calculate_vmt_fees")
            simulation_models.remove("subsidized_residential_feasibility")
            simulation_models.remove("subsidized_residential_developer_vmt")

        if not run_setup["run_jobs_housing_fee_strategy"]:
            simulation_models.remove("calculate_jobs_housing_fees")
        #    simulation_models.remove["subsidized_residential_feasibility"]
        #    simulation_models.remove["subsidized_residential_developer_jobs_housing"]        

        simulation_validation_models = [
            "simulation_validation"
        ]
        
        simulation_summary_models = [
            "interim_zone_output",
            "disaggregate_output",
            "new_buildings_summary",
            "parcel_growth_summary",
            "hazards_slr_summary",
            "hazards_eq_summary",

            "deed_restricted_units_summary",
            "deed_restricted_units_growth_summary",

            "geographic_summary",
            "geographic_growth_summary",
            "parcel_transitions",
            "taz1_summary",
            "maz_marginals",
            "maz_summary",
            "taz2_marginals",
            "county_marginals",
            "region_marginals",
            "taz1_growth_summary",
            "maz_growth_summary",
        ]
                    
        simulation_metrics_models = [
            "growth_geography_metrics",
            "deed_restricted_units_metrics",
            "household_income_metrics",
            "equity_metrics",
            "jobs_housing_metrics",
            "jobs_metrics",
            "slr_metrics",
            "earthquake_metrics",
            "greenfield_metrics",
        ]

        simulation_visualization_models = [
            "copy_files_to_viz_loc",
            "add_to_model_run_inventory_file"
        ]

        if run_setup["run_summaries"]:
            baseyear_models.extend(baseyear_summary_models)
        if run_setup["run_metrics"]:
            baseyear_models.extend(baseyear_metrics_models)
        if SLACK: baseyear_models.append('slack_simulation_status')

        # 2010-based setup has a bunch of specialized baseyear models
        # For 2020-based run, we'll stop doing that (if possible)
        if BASE_YEAR==2010:
            print("Running baseyear_models {} for years {}".format(baseyear_models,BASE_YEAR))
            orca.run(baseyear_models, iter_vars=[BASE_YEAR])

            years_to_run = range(BASE_YEAR+EVERY_NTH_YEAR, STOP_YEAR+1, EVERY_NTH_YEAR)
        else:
            # run normal set starting in 2020
            years_to_run = range(BASE_YEAR, STOP_YEAR+1, EVERY_NTH_YEAR)

        if run_setup["run_summaries"]:
            simulation_models.extend(simulation_summary_models)
        if run_setup["run_metrics"]:
            simulation_models.extend(simulation_metrics_models)
        if run_setup["run_simulation_validation"]:
            simulation_models.extend(simulation_validation_models)
        if SLACK: simulation_models.append('slack_simulation_status')

        print("Running simulation_models {} for years {}".format(simulation_models,years_to_run))
        orca.run(simulation_models, iter_vars=years_to_run)

        if run_setup["run_visualizer"]:
            orca.run(simulation_visualization_models, iter_vars=[STOP_YEAR])
            

    elif mode == "visualizer":

        orca.run([
                "copy_files_to_viz_loc",
                "add_to_model_run_inventory_file"
        ])

    else:
        raise "Invalid mode"


if ASANA:
    # We can do this before the shutil copy step and just use the native run_setup.yaml in the same dir as baus.py
    task_handle = create_asana_task_from_yaml(options.run_setup_yaml, run_name, ASANA_SECTION_NAME)

    # Get task identifer for later comment posting 
    task_gid = task_handle['gid']

    logger.info(f"Creating asana run task with URL: {task_handle['permalink_url']}")

# Memorialize the run config with the outputs - goes by run name attribute

logger.info(f'***Copying {options.run_setup_yaml} to output directory')
shutil.copyfile(options.run_setup_yaml, os.path.join(orca.get_injectable("outputs_dir"), f'run_setup_{run_name}.yaml'))

if SLACK: baus.slack.slack_start(MODE, host, run_name, run_setup)
    
if ASANA:
    asana_msg = f"Creating asana run task with URL: {task_handle['permalink_url']}"
    asana_response = client.chat_postMessage(channel=orca.get_injectable('slack_channel'),
        thread_ts=orca.get_injectable('slack_init_response').data['ts'],
        text=asana_msg)

# main event: run the models
try:
    run_models(MODE)

except Exception as e:
    logger.info(traceback.print_exc())
    tb = e.__traceback__
    
    traces = traceback.extract_tb(tb=tb) #,limit=1)
    error_type = type(e).__name__
    error_msg = str(e)

    # collect the traceback 
    error_msgs = []
    for file_name, line_number, function_name, _ in traces:
        this_trace = f'{pathlib.Path(file_name).name} - line {line_number}, func {function_name}'
        error_msgs.append(this_trace)
    # we care mostly about the triggering error - so in reverse order
    error_msgs.reverse()
    error_trace = '\n'.join(error_msgs)
    logger.info(error_trace)

    if SLACK: baus.slack.slack_error(error_type, error_msg, error_trace)

    if ASANA:
        # Add a fail comment
        add_comment_to_task(task_gid, error_msg)


    else:
        raise e
    sys.exit(0)

if SLACK: baus.slack.slack_complete(MODE, host, run_name)

if ASANA:
    # Add a comment
    add_comment_to_task(task_gid, "Simulation completed successfully.")

    # Mark the task as completed
    mark_task_as_complete(task_gid)

    response = client.chat_postMessage(channel=slack_channel,
                                       thread_ts=orca.get_injectable('slack_init_response').data['ts'],
                                       text='Check asana for details.')

print("Finished", time.ctime())
