from __future__ import print_function
import os
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
import numpy as np
import pandas as pd
import orca
import socket
import argparse
import urbansim
import urbansim_defaults
import orca
import orca_test
import pandana
import shutil
import logging

from logging_setup import setup_logging  # Custom logging setup function
logger = logging.getLogger(__name__)


MODE = "simulation"
EVERY_NTH_YEAR = 5
IN_YEAR, OUT_YEAR = 2010, 2050
years_to_run = range(IN_YEAR+EVERY_NTH_YEAR, OUT_YEAR+1, EVERY_NTH_YEAR)
        
CURRENT_BRANCH = os.popen('git rev-parse --abbrev-ref HEAD').read().rstrip()
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read().rstrip()

SLACK = "URBANSIM_SLACK" in os.environ
if SLACK:
    host = socket.gethostname()
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    client = WebClient(token=os.environ["SLACK_TOKEN"])
    slack_channel = "#urbansim_sim_update"


parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument('--mode', action='store', dest='mode', help='which mode to run (see code for mode options)')
parser.add_argument('-i', action='store_true', dest='interactive', help='enter interactive mode after imports')
parser.add_argument('--set-random-seed', action='store_true', dest='set_random_seed', help='set a random seed for consistent stochastic output')
parser.add_argument('--disable-slack', action='store_true', dest='no_slack', help='disable slack outputs')
parser.add_argument('--enable-asana', action='store_true', dest='use_asana', default=False, help='disable Asana task creation')


options = parser.parse_args()

if options.interactive:
    INTERACT = True

if options.mode:
    MODE = options.mode

if options.set_random_seed:
    SET_RANDOM_SEED = True
    np.random.seed(42)
else:
    SET_RANDOM_SEED = False

if options.no_slack:
    SLACK = False

if options.use_asana:
    ASANA = True
    
    from scripts.meta.asana_utils import (
    create_asana_task_from_yaml,
    add_comment_to_task, 
    mark_task_as_complete)
    ASANA_SECTION_NAME = 'Final Blueprint Runs'
else:
    ASANA = False

orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)
orca.add_injectable("base_year", IN_YEAR)
orca.add_injectable("final_year", OUT_YEAR)

run_setup = orca.get_injectable("run_setup")
run_name = orca.get_injectable("run_name")
outputs_dir = pathlib.Path(orca.get_injectable("outputs_dir"))
outputs_dir.mkdir(parents=True, exist_ok=True)

# Get the outputs directory and run name
log_file_path = os.path.join(orca.get_injectable("outputs_dir"), f"{run_name}.log")

# Set up logging to write to the specified log file
setup_logging(log_file_path)

# Example log message to confirm logging is working
logger = logging.getLogger(__name__)
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



def run_models(mode, run_setup, years_to_run):

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

        def get_baseyear_models():

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

            return baseyear_models
    
        def get_baseyear_summary_models():

            baseyear_summary_models = [

                "simulation_validation",

                "parcel_summary",
                "building_summary",

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

            return baseyear_summary_models

        def get_baseyear_metrics_models():
            
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

            return baseyear_metrics_models
    
        def get_simulation_models():
        
            simulation_models = [

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
        #        "subsidized_residential_feasibility",
        #        "subsidized_residential_developer_jobs_housing",

                #"alt_feasibility",
                "residential_developer",
                "developer_reprocess",

                #"alt_feasibility",
                
                "retail_developer",

                #"alt_feasibility",
                
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
                "calculate_jobs_housing_fees"]

            if not run_setup["run_jobs_to_transit_strategy_elcm"]:
                simulation_models.remove("elcm_simulate_ec5")
                logger.info('Removing `elcm_simulate_ec5`')

            if not run_setup["run_jobs_to_transit_strategy_random"]:
                simulation_models.remove("gov_transit_elcm")
                logger.info('Removing `gov_transit_elcm`')
            
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

            return simulation_models
        

        def get_simulation_validation_models():

            simulation_validation_models = [
                "simulation_validation"
            ]

            return simulation_validation_models
        

        def get_simulation_summary_models():

            simulation_summary_models = [

                "interim_zone_output",
                "new_buildings_summary",

                "parcel_summary",
                "parcel_growth_summary",
                "building_summary",

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

            return simulation_summary_models
        

        def get_simulation_metrics_models():
            
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

            return simulation_metrics_models
        
        def get_simulation_visualization_models():

            simulation_visualization_models = [
                "copy_files_to_viz_loc",
                "add_to_model_run_inventory_file"
            ]

            return simulation_visualization_models

        baseyear_models = get_baseyear_models()
        if run_setup["run_summaries"]:
            baseyear_models.extend(get_baseyear_summary_models())
        if run_setup["run_metrics"]:
            baseyear_models.extend(get_baseyear_metrics_models())
        orca.run(baseyear_models, iter_vars=[years_to_run[0]])

        simulation_models = get_simulation_models()
        if run_setup["run_summaries"]:
            simulation_models.extend(get_simulation_summary_models())
        if run_setup["run_metrics"]:
            simulation_models.extend(get_simulation_metrics_models())
        if run_setup["run_simulation_validation"]:
            simulation_models.extend(get_simulation_validation_models())
        orca.run(simulation_models, iter_vars=years_to_run)

        if run_setup["run_visualizer"]:
            visualization_models = get_simulation_visualization_models()
            orca.run(visualization_models, iter_vars=[years_to_run[-1]])
            

    elif mode == "visualizer":

        orca.run([
                "copy_files_to_viz_loc",
                "add_to_model_run_inventory_file"
        ])

    else:
        raise "Invalid mode"




if ASANA:
    # We can do this before the shutil copy step and just use the native run_setup.yaml in the same dir as baus.py
    task_handle = create_asana_task_from_yaml('run_setup.yaml', run_name, ASANA_SECTION_NAME)

    # Get task identifer for later comment posting 
    task_gid = task_handle['gid']

    logger.info(f"Creating asana run task with URL: {task_handle['permalink_url']}")

# Memorialize the run config with the outputs - goes by run name attribute

logger.info('***Copying run_setup.yaml to output directory')
shutil.copyfile("run_setup.yaml", os.path.join(orca.get_injectable("outputs_dir"), f'run_setup_{run_name}.yaml'))


if SLACK and MODE == "estimation":
    slack_start_message = f'Starting estimation {run_name} on host {host}'
    try:
        # For first slack channel posting of a run, catch any auth errors
        init_response = client.chat_postMessage(channel=slack_channel,
                                                text=slack_start_message)
    except SlackApiError as e:
        assert e.response["ok"] is False
        assert e.response["error"]  
        logger.info(f"Slack Channel Connection Error: {e.response['error']}")

if SLACK and MODE == "simulation":
    slack_start_message = f'Starting simulation {run_name} on host {host}\nOutput written to: {run_setup["outputs_dir"]}'
    
    try:
        # For first slack channel posting of a run, catch any auth errors
        init_response = client.chat_postMessage(channel=slack_channel,
                                           text=slack_start_message)

        if ASANA:

            asana_msg = f"Creating asana run task with URL: {task_handle['permalink_url']}"
            asana_response = client.chat_postMessage(channel=slack_channel,
                                    thread_ts=init_response.data['ts'],
                                    text=asana_msg)

    except SlackApiError as e:
        assert e.response["ok"] is False
        assert e.response["error"]  
        logger.info(f"Slack Channel Connection Error: {e.response['error']}")

try:
    run_models(MODE, run_setup, years_to_run)
    
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

    if SLACK and MODE == "simulation":
        slack_fail_message = f'DANG!  Simulation failed for {run_name} on host {host} with the error of type "{error_type}", and message {error_msg}. Deets here:\n{error_trace}'
        
        response = client.chat_postMessage(channel=slack_channel,
                                           thread_ts=init_response.data['ts'],
                                           text=slack_fail_message)

        if ASANA:
            # Add a fail comment
            add_comment_to_task(task_gid, slack_fail_message)


    else:
        raise e
    sys.exit(0)

if SLACK and MODE == "simulation":
    slack_completion_message = f'Completed simulation {run_name} on host {host}'
    response = client.chat_postMessage(channel=slack_channel,
                                       thread_ts=init_response.data['ts'],
                                       text=slack_completion_message)

    
    if ASANA:
        # Add a comment
        add_comment_to_task(task_gid, "Simulation completed successfully.")

        # Mark the task as completed
        mark_task_as_complete(task_gid)

        response = client.chat_postMessage(channel=slack_channel,
                                       thread_ts=init_response.data['ts'],
                                       text='Check asana for details.')


    


                                                                                            
logger.info("Finished: %s", time.ctime())
