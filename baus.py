from __future__ import print_function
import os
import sys
import time
import traceback
from baus import models
from baus import slr
from baus import earthquake
from baus import ual
from baus import validation
import numpy as np
import pandas as pd
import orca
import socket
import argparse
import warnings
from baus.utils import compare_summary



###  VARIABLES ###

BRANCH = os.popen('git rev-parse --abbrev-ref HEAD').read()
CURRENT_COMMIT = os.popen('git rev-parse HEAD').read() 
LOGS = True
SLACK = "URBANSIM_SLACK" in os.environ
RANDOM_SEED = True
MODE = "simulation"
EVERY_NTH_YEAR = 5
IN_YEAR, OUT_YEAR = 2010, 2050
S3 = False


### INJECTABLES ### 

orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)
orca.add_injectable("base_year", IN_YEAR)
orca.add_injectable("slack_enabled", SLACK)


### PARSER ARGS ###

parser = argparse.ArgumentParser(description='Run UrbanSim models.')

parser.add_argument('-y', action='store', dest='out_year', type=int,
                    help='The year to which to run the simulation.')

parser.add_argument('--mode', action='store', dest='mode',
                    help='which mode to run (see code for mode options)')

parser.add_argument('--random-seed', action='store_true', dest='random_seed',
                    help='set a random seed for consistent stochastic output')

parser.add_argument('--disable-slack', action='store_true', dest='noslack',
                    help='disable slack outputs')


### OPTIONS ###

options = parser.parse_args()

if options.out_year:
    OUT_YEAR = options.out_year

if options.mode:
    MODE = options.mode

if options.random_seed:
    RANDOM_SEED = True

if options.noslack:
    SLACK = False


### ADDITIONAL SETTINGS ###

if LOGS:
    print('***The Standard stream is being written to /runs.log***')
    sys.stdout = sys.stderr = open("runs/run.log", 'w')

if RANDOM_SEED:
    np.random.seed(12)

if SLACK:
    from slacker import Slacker
    slack = Slacker(os.environ["SLACK_TOKEN"])
    host = socket.gethostname()


### MODES AND THEIR SUB-MODELS ###

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

        ], iter_vars=[2010])

        orca.run([
            "load_rental_listings", 
            "neighborhood_vars",    
            "regional_vars",        
            "rrh_estimate",         
            "hlcm_owner_estimate",  
            "hlcm_renter_estimate", 
        ])
 

    elif MODE == "preprocessing":

        orca.run([
            "preproc_jobs",
            "preproc_households",
            "preproc_buildings",
            "initialize_residential_units"
        ])


    elif MODE == "debug":

        orca.run(["simulation_validation"], [2010])


    elif MODE == "feasibility":

        orca.run([

            "neighborhood_vars",          
            "regional_vars",                
            "rsh_simulate",                 
            "nrh_simulate",                 
            "price_vars",
            "subsidized_residential_feasibility"

        ], iter_vars=[2010])

        df = orca.get_table("feasibility").to_frame()
        df = df.stack(level=0).reset_index(level=1, drop=True)
        df.to_csv("output/feasibility.csv")


    elif MODE == "simulation":

    	# BASE YEAR MODELS #
        if IN_YEAR:
            orca.run([

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
                "price_vars",

                "simulation_validation",
                "parcel_summary",
                "building_summary",
                "geographic_summary",
                "travel_model_output",
                "travel_model_2_output",
                "hazards_slr_summary",
                "hazards_eq_summary",
                "diagnostic_output",
                "config",
                "slack_report"

            ], iter_vars=[IN_YEAR])

        # FORECAST YEAR MODELS #
   		else:
			def get_simulation_models():
			    
			    models = [

			    	# INITIAL MODELS AND CALCS #

			    	# sea level rise models
			        "slr_inundate",
			        "slr_remove_dev",
			        # earthquake models
			        "eq_code_buildings",
			        "earthquake_demolish",

			        # accessibility calcs
			        "neighborhood_vars",    
			        "regional_vars",        

			        # non-residential rent price model
			        "nrh_simulate",         

			        # household relocation rates and new households
			        "household_relocation",
			        "households_transition",
			        "reconcile_unplaced_households",
			        # job relocation rates and new jobs
			        "jobs_relocation",
			        "jobs_transition",

			        # housing price calculations
			        "balance_rental_and_ownership_hedonics",
			        "price_vars",

			        # CREATE BUILDINGS #

			        # development pipeline
			        "scheduled_development_events",

			        # preserve some units
			        "preserve_affordable",
			        # total the money available for subsidized residential development
			        "lump_sum_accounts",
			        "subsidized_residential_developer_lump_sum_accts",
			        # count the money available for subsidized commercial development
			        "office_lump_sum_accounts",
			        "subsidized_office_developer_lump_sum_accts",

			        # run feasibility, then run the models that use the subsidized accounts
			        "alt_feasibility",
			        "subsidized_residential_feasibility",
			        "subsidized_residential_developer_vmt",
					"subsidized_residential_feasibility",
			        "subsidized_residential_developer_jobs_housing",

			        # run the core developer models
			        "residential_developer",
			        "developer_reprocess",
			        "retail_developer",
			        "office_developer",

			        # additional 
			        "subsidized_office_developer_vmt",
			        "accessory_units",
			        "calculate_vmt_fees",

			        # update unit counts
			        "remove_old_units",
			        "initialize_new_units",
			        "reconcile_unplaced_households",

			        # residential rent and sale price models
			        "rsh_simulate",
			        "rrh_simulate",

			        # assign tenure
			        "assign_tenure_to_new_units",

			        # PLACE AGENTS IN BUILDINGS #

			        # Q1 households can choose deed-restricted units first, 
			        # then unplaced Q1 + Q2, Q3, Q4 are placed in deed-restricted or market-rate
			        "hlcm_owner_lowincome_simulate",
			        "hlcm_renter_lowincome_simulate",

			        # owners choose from owner owner units, renters choose from renter units
			        "hlcm_owner_simulate",
			        "hlcm_renter_simulate",

			        # force placement of any unplaced households
			        "hlcm_owner_simulate_no_unplaced",
			        "hlcm_owner_lowincome_simulate_no_unplaced",
			        "hlcm_renter_simulate_no_unplaced",
			        "hlcm_renter_lowincome_simulate_no_unplaced",
			        "reconcile_placed_households",

			        # add to some job sectors proportionally, then run standard job placement models
			        "proportional_elcm",        
			        "elcm_simulate",           

			        # WRITE SUMMARIES #

			        "topsheet",
			        "simulation_validation",
			        "parcel_summary",
			        "building_summary",
			        "diagnostic_output",
			        "calculate_vmt_fees",
			        "calculate_jobs_housing_fees",
			        "geographic_summary",
			        "travel_model_output",
			        "travel_model_2_output",
			        "hazards_slr_summary",
			        "hazards_eq_summary",
			        "slack_report"

			        ]

			    return models


	        years_to_run = range(IN_YEAR+EVERY_NTH_YEAR, OUT_YEAR+1, EVERY_NTH_YEAR)
	        models = get_simulation_models()
	        orca.run(models, iter_vars=years_to_run)

    else:
        raise "Invalid mode"


### RUN MODEL, PRINT KEY INFO AND SLACK MESSAGES ###

print("Started", time.ctime())
print("Current Branch : ", BRANCH.rstrip())
print("Current Commit : ", CURRENT_COMMIT.rstrip())
print("Random Seed : ", RANDOM_SEED)

if SLACK and MODE == "simulation":
    slack.chat.post_message(
        '#urbansim_sim_update',
        'Starting simulation on host %s' % host, as_user=True)
try:
    run_models(MODE)

except Exception as e:
    print(traceback.print_exc())
    if SLACK and MODE == "simulation":
        slack.chat.post_message(
            '#urbansim_sim_update',
            'DANG!  Simulation failed on host %s'
            % (host), as_user=True)
    else:
        raise e
    sys.exit(0)

print("Finished", time.ctime())

if SLACK and MODE == "simulation":
    slack.chat.post_message(
        '#urbansim_sim_update',
        'Completed simulation on host %s' % (host), as_user=True)