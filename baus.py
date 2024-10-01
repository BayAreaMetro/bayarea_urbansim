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
from baus.summaries import \
    core_summaries, geographic_summaries, affordable_housing_summaries, \
    hazards_summaries, metrics, travel_model_summaries
from baus.visualizer import push_model_files
import baus.slack
import baus.debug
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


# for log file
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 500)

RUN_SETUP_YAML = "run_setup.yaml" # overriden by arg
MODE = "simulation"

SLACK = "URBANSIM_SLACK" in os.environ
if SLACK:
    host = socket.gethostname()
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    client = WebClient(token=os.environ["SLACK_TOKEN"])
    slack_channel = "#urbansim_sim_update"
    orca.add_injectable('slack_client',client)
    orca.add_injectable('slack_channel',slack_channel)

SET_RANDOM_SEED = True
if SET_RANDOM_SEED:
    np.random.seed(12)


parser = argparse.ArgumentParser(description='Run UrbanSim models.')
parser.add_argument('--run_setup_yaml', help='Specify run_setup.yaml file to use')
parser.add_argument('--mode', action='store', dest='mode', help='which mode to run (see code for mode options)')
parser.add_argument('-i', action='store_true', dest='interactive', help='enter interactive mode after imports')
parser.add_argument('--set-random-seed', action='store_true', dest='set_random_seed', help='set a random seed for consistent stochastic output')
parser.add_argument('--disable-slack', action='store_true', dest='no_slack', help='disable slack outputs')

options = parser.parse_args()

if options.run_setup_yaml:
    RUN_SETUP_YAML = options.run_setup_yaml

if options.interactive:
    INTERACT = True

if options.mode:
    MODE = options.mode

if options.set_random_seed:
    SET_RANDOM_SEED = True

if options.no_slack:
    SLACK = False

orca.add_injectable("run_setup_yaml", RUN_SETUP_YAML)

run_setup = orca.get_injectable("run_setup")
run_name = orca.get_injectable("run_name")
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

def run_models(MODE):

    if MODE == "estimation":

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


    elif MODE == "preprocessing":

        orca.run([
            "preproc_jobs",
            "preproc_households",
            "preproc_buildings",
            "initialize_residential_units"
        ])


    elif MODE == "simulation":

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
            

    elif MODE == "visualizer":

        orca.run([
                "copy_files_to_viz_loc",
                "add_to_model_run_inventory_file"
        ])

    else:
        raise "Invalid mode"

# urbansim has logging module code...
# logging_filename = pathlib.Path(orca.get_injectable("outputs_dir")) / f"{run_name}_debug.log"
# urbansim.utils.logutil.log_to_file(str(logging_filename), level=logging.DEBUG)
# urbansim.utils.logutil.log_to_stream(level=logging.DEBUG)
# urbansim.utils.logutil.set_log_level(level=logging.DEBUG)

print('***The Standard stream is being written to {}.log***'.format(run_name))
sys.stdout = sys.stderr = open(os.path.join(orca.get_injectable("outputs_dir"), "%s.log") % run_name, 'w')

# Memorialize the run config with the outputs - goes by run name attribute

print('***Copying {} to output directory'.format(RUN_SETUP_YAML))
shutil.copyfile(RUN_SETUP_YAML, os.path.join(orca.get_injectable("outputs_dir"), f'run_setup_{run_name}.yaml'))

print("Started", time.ctime())
print("Current Branch : ", os.popen('git rev-parse --abbrev-ref HEAD').read().rstrip())
print("Current Commit : ", os.popen('git rev-parse HEAD').read().rstrip())
print("Set Random Seed : ", SET_RANDOM_SEED)
print("python version: %s" % sys.version.split('|')[0])
print("urbansim version: %s" % urbansim.__version__)
print("urbansim_defaults version: %s" % urbansim_defaults.__version__)
print("orca version: %s" % orca.__version__)
print("orca_test version: %s" % orca_test.__version__)
print("pandana version: %s" % pandana.__version__)
print("numpy version: %s" % np.__version__)
print("pandas version: %s" % pd.__version__)

print("SLACK: {}".format(SLACK))
print("MODE: {}".format(MODE))

if SLACK: baus.slack.slack_start(MODE, host, run_name, run_setup)

try:
    run_models(MODE)
except Exception as e:
    print(traceback.print_exc())
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
    print(error_trace)

    if SLACK: baus.slack.slack_error(error_type, error_msg, error_trace)
    
    raise e
    sys.exit(0)

if SLACK: baus.slack.slack_complete(MODE, host, run_name)                                                                                      
print("Finished", time.ctime())         
