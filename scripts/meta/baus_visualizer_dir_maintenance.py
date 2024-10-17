#!/usr/bin/env python
# coding: utf-8

import argparse
import logging
import os
import shutil
from pathlib import Path

import pandas as pd


def setup_logging(viz_dir_prod):
    LOG_FILENAME = 'viz_dir_file_logger.log'
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(ch)

    # File handler - info
    fh_info = logging.FileHandler(viz_dir_prod / LOG_FILENAME, mode='w')
    fh_info.setLevel(logging.INFO)
    fh_info.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh_info)

    # File handler - debug
    fh_debug = logging.FileHandler(viz_dir_prod / LOG_FILENAME, mode='w')
    fh_debug.setLevel(logging.DEBUG)
    fh_debug.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
    logger.addHandler(fh_debug)

    return logger

def m_swap(path):
    if path[:3] in ["M:/", "M:\\"]:
        new_path = M_DRIVE / path[3:]
        return new_path
    else:
        return Path(path)

def runlog_path_creator(run_log, years=[2020, 2050, 'growth']):
    run_log['core_summaries_path'] = run_log.apply(
        lambda x: f'{x.outputs_dir}/{x.run_name}/core_summaries', axis=1).map(m_swap)

    for year in years:
        run_log[f'buildings_path_{year}'] = run_log.apply(
            lambda x: f'{x.outputs_dir}/{x.run_name}/core_summaries/{x.run_name}_building_summary_{year}.csv', axis=1).map(m_swap)
        run_log[f'parcels_path_{year}'] = run_log.apply(
            lambda x: f'{x.outputs_dir}/{x.run_name}/core_summaries/{x.run_name}_parcel_summary_{year}.csv', axis=1).map(m_swap)
        allyears = 'allyears' if year == 'growth' else year
        run_log[f'zone_interim_path_{year}'] = run_log.apply(
            lambda x: f'{x.outputs_dir}/{x.run_name}/core_summaries/{x.run_name}_interim_zone_output_{allyears}.csv', axis=1).map(m_swap)
        run_log[f'zone_path_{year}'] = run_log.apply(
            lambda x: f'{x.outputs_dir}/{x.run_name}/travel_model_summaries/{x.run_name}_taz1_summary_{year}.csv', axis=1).map(m_swap)
        for geo in ['county', 'superdistrict', 'juris']:
            run_log[f'{geo}_path_{year}'] = run_log.apply(
                lambda x: f'{x.outputs_dir}/{x.run_name}/geographic_summaries/{x.run_name}_{geo}_summary_{year}.csv', axis=1).map(m_swap)

    run_log['redev_path'] = run_log.apply(
        lambda x: f'{x.outputs_dir}/{x.run_name}/redevelopment_summaries/{x.run_name}_county_redev_summary_growth.csv', axis=1).map(m_swap)

def write_list_to_file(data, path):
    with open(path, 'w') as file:
        file.write("run_name\n")
        for item in data:
            file.write(f"{item}\n")

def generate_visualizer_files_from_run_name(run_names, viz_dir_prod, viz_dir_storage):
    """
    Generate a dictionary of expected file paths for the given run names.
    
    Args:
    run_names (list of str): A list of BAUS run names to process.
    viz_dir_prod (Path): Path to the production visualizer directory.
    viz_dir_storage (Path): Path to the storage visualizer directory.
    
    Returns:
    dict: A nested dictionary with the structure:
          {run_name: {component_key: {storage_key: file_path}}}
          where component_key is one of the keys in component_paths
          for a particular summary level, storage_key is one of the
          keys in storage_type, and file_path is the full path to 
          the BAUS run and summary level specific datafile.
    """
    # Define the storage types and their corresponding directories
    storage_type = {'PROD': viz_dir_prod, 'STORAGE': viz_dir_storage}

    # Initialize an empty dictionary to store paths for each run name
    l_0 = {}

    # Loop through each run name to generate paths
    for run_name in run_names:
        # Define the expected file components for each run name
        component_paths = {
            "new_buildings": f"{run_name}_new_buildings_summary.csv",
            "taz": f"{run_name}_taz1_summary_growth.csv",
            "interim_zone_output": f"{run_name}_interim_zone_output_allyears.csv",
            "juris_dr": f"{run_name}_juris_dr_growth.csv",
            "county_dr": f"{run_name}_county_dr_growth.csv",
            "superdistrict_dr": f"{run_name}_superdistrict_dr_growth.csv",
            "juris_summary": f"{run_name}_juris_summary_growth.csv",
            "county_summary": f"{run_name}_county_summary_growth.csv",
            "superdistrict_summary": f"{run_name}_superdistrict_summary_growth.csv"
        }

        # Initialize a dictionary to hold paths for each component of the current run name
        l_1 = {}
        for component_key, component_value in component_paths.items():
            # Initialize a dictionary to hold paths for each storage type of the current component
            l_2 = {}
            for storage_key, storage_value in storage_type.items():
                # Combine the storage directory and the component file name to form the full path
                this_combo = storage_value / component_value
                l_2[storage_key] = this_combo
            
            # Add the component paths to the dictionary for the current run name
            l_1[component_key] = l_2
        
        # Add the paths for the current run name to the main dictionary
        l_0[run_name] = l_1

    logging.info(f'Generated summary files for {len(l_0)} runs')
    return l_0


def clear_prod_files(run_names, viz_dir_prod, viz_dir_storage, viz_dir_overflow):
    """
    Move unnecessary or duplicate files from the production directory to storage or overflow.

    Args:
    run_names (list of str): A list of run names to process.
    viz_dir_prod (Path): Path to the production visualizer directory.
    viz_dir_storage (Path): Path to the storage visualizer directory.
    viz_dir_overflow (Path): Path to the overflow directory for duplicate files.
    """
    # Generate the expected file paths for the given run names
    desired_run_files = generate_visualizer_files_from_run_name(run_names, viz_dir_prod, viz_dir_storage)
    logging.info(f'Found {len(desired_run_files)} expected runs out of {len(run_names)} provided')

    # Use rglob to find all .csv files in the production directory and its subdirectories
    prod_files = viz_dir_prod.rglob('*.csv')
    
    for f in prod_files:
        # Skip files that are part of the 'model_run_inventory'
        if 'model_run_inventory' not in f.name:
            # Define the target path in the storage directory
            target_path = viz_dir_storage / f.name
            try:
                if not target_path.exists():
                    # If the file does not exist in storage, move it from production to storage
                    logging.info(f'MOVING to STORAGE:\n\t{f.name}')
                    shutil.move(f, viz_dir_storage)
                else:
                    # If the file exists in storage, compare sizes to determine duplicates
                    if f.stat().st_size == target_path.stat().st_size:
                        logging.info(f'File already in storage; moving to OVERFLOW - probably safe to delete\n\t{f.name}')
                        # Move duplicate files to the overflow directory
                        shutil.move(f, viz_dir_overflow)
            except FileNotFoundError:
                logging.error(f'File not found: {f}')
            except OSError as e:
                logging.error(f'Error moving file {f}: {e}')


def move_desired_run_files(run_names, viz_dir_prod, viz_dir_storage):
    """
    Move specified run files from the storage directory to the production directory if they don't already exist in production.

    Args:
    run_names (list of str): A list of run names to process.
    viz_dir_prod (Path): Path to the production visualizer directory.
    viz_dir_storage (Path): Path to the storage visualizer directory.
    """
    
    # Generate the expected paths for the desired run files from their run names
    desired_run_files = generate_visualizer_files_from_run_name(run_names, viz_dir_prod, viz_dir_storage)
    
    # Iterate over each run name and its associated component paths
    for run_name, component_dict in desired_run_files.items():
        for component, paths in component_dict.items():
            logging.info(f'Processing {run_name} - {component} files...')
            try:
                # Check if the file does not exist in the production directory
                if not paths["PROD"].exists():
                    logging.info(f'   File not found in PROD: {paths["PROD"].name}. Moving from STORAGE to PROD.')
                    # Move the file from the storage directory to the production directory
                    shutil.move(paths["STORAGE"], paths["PROD"])
                else:
                    logging.info(f'   File already exists in PROD: {paths["PROD"].name}. No action needed.')
            except FileNotFoundError:
                # Log debug info if the file is not found in the storage directory
                logging.debug(f'   File not found in STORAGE: {paths["STORAGE"].name}')
            except OSError as e:
                # Log debug info if there's an error accessing or moving the file
                logging.debug(f'   Cannot move/access file: {paths["STORAGE"].name}. Error: {e}')

def main():
    parser = argparse.ArgumentParser(description='Maintain BAUS Visualizer Output Files.')
    parser.add_argument('--viz_dir_prod', type=str, default='/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/viz_test/PROD_BAUS_Visualizer_PBA50Plus_Files', help='Path to the production visualizer directory.')
    parser.add_argument('--viz_dir_storage', type=str, default='/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/viz_test/STORAGE_BAUS_Visualizer_PBA50Plus_Files', help='Path to the storage visualizer directory.')
    parser.add_argument('--viz_dir_overflow', type=str, default='/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/viz_test/OVERFLOW', help='Path to the overflow directory.')
    parser.add_argument('--run_log_path', type=str, default='/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/run_setup_tracker_autogen.csv', help='Path to the run log CSV file.')
    parser.add_argument('--runs', nargs='*', default=['PBA50_FinalBlueprint_Exogenous_v2', 'PBA50_FBP', 'PBA50_NoProject_Exogenous'], help='List of run names to process.')

    args = parser.parse_args()

    global M_DRIVE
    M_DRIVE = Path("/Volumes/Data")

    viz_dir_prod = m_swap(args.viz_dir_prod)
    viz_dir_storage = m_swap(args.viz_dir_storage)

    # TODO: move to remove the overflow - that was a temp stage to make sure we wouldn't accidentally lose files
    viz_dir_overflow = m_swap(args.viz_dir_overflow)
    run_log_path = m_swap(args.run_log_path)
    #model_run_inventory_path = m_swap(args.model_run_inventory_path)
    run_names = args.runs

    logger = setup_logging(viz_dir_prod)
    
    run_log = pd.read_csv(run_log_path)
    logging.info('Auto-generated run log imported')

    # adorn the run log with full paths to the different visualizer files
    runlog_path_creator(run_log)

    # this writes out the subset of the inventory passed to the --runs argument - those are the ones Tableau will show
    #run_log.to_csv(viz_dir_prod / "model_run_inventory.csv", index=False)
    #logging.info(f'Run log saved to {viz_dir_prod / "model_run_inventory.csv"}')

    # this writes out the subset of the inventory passed to the --runs argument - those are the ones Tableau will show
    write_list_to_file(run_names, viz_dir_prod / "model_run_inventory.csv")
    logging.info(f'Run names saved to {viz_dir_prod / "model_run_inventory.csv"}')

    # TODO: we should check if a file in the runs argument is already in production - if so no need to remove those.
    clear_prod_files(run_names, viz_dir_prod, viz_dir_storage, viz_dir_overflow)
    move_desired_run_files(run_names, viz_dir_prod, viz_dir_storage)

    # example: 
    # python baus_visualizer_dir_maintenance.py \
    # --viz_dir_prod "/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/viz_test/PROD_BAUS_Visualizer_PBA50Plus_Files" \
    # --viz_dir_storage "/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/viz_test/STORAGE_BAUS_Visualizer_PBA50Plus_Files" \
    # --viz_dir_overflow "/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/viz_test/OVERFLOW" \
    # --run_log_path "/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/run_setup_tracker_autogen.csv" \
    # --runs 'PBA50_FBP' 'PBA50Plus_Draft_Blueprint_v8_znupd_nodevfix' 'PBA50Plus_NoProject_v13_zn_revisit_ugb'


if __name__ == "__main__":
    main()
