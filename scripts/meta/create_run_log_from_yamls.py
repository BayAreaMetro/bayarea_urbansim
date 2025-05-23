import pandas as pd
from pathlib import Path
import pathlib
import yaml
import datetime
import os
from typing import Optional, Dict, Tuple, List, Any
import logging

# Setup basic logging for the script itself
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def get_log_file_path_from_yaml(yaml_path: Path) -> Path:
    """
    Constructs the path to the log file from a run setup YAML file path.

    Assumes the log file shares the same stem as the YAML file,
    but with a .log extension, and potentially without prefixes/suffixes
    containing "run_setup".

    Args:
        yaml_path (Path): The path to the run setup YAML file.

    Returns:
        Path: The path to the corresponding log file.
    """
    log_file_name = f'{yaml_path.stem}.log'.replace('_run_setup', '').replace('run_setup_', '')
    return yaml_path.parent / log_file_name


def parse_log_file(log_path: Path) -> Dict[str, Any]:
    """
    Parses a log file to extract timestamps, Git details, and log style.
    Reads the file only once.

    Args:
        log_path (Path): The path object to the log file.

    Returns:
        dict: A dictionary containing 'start_time', 'end_time',
              'branch', 'commit', and 'is_new_style'.
              Returns None for values if not found or if the file is empty/invalid.
    """
    results = {
        'start_time': None,
        'end_time': None,
        'branch': None,
        'commit': None,
        'is_new_style': False,
        'error': None  # To store any parsing errors or file issues
    }

    if not log_path.exists():
        results['error'] = f"Log file not found: {log_path}"
        logging.warning(results['error'])
        return results

    if log_path.stat().st_size == 0:
        results['error'] = f"Log file is empty: {log_path}"
        logging.warning(results['error'])
        return results

    try:
        with open(log_path, "r", encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        results['error'] = f"Error reading log file {log_path}: {e}"
        logging.error(results['error'])
        return results

    if not lines: # Should be caught by st_size == 0, but as a safeguard
        results['error'] = f"Log file is empty (no lines): {log_path}"
        logging.warning(results['error'])
        return results

    log_series = pd.Series(lines)

    # Determine log style (new or old)
    if log_series.str.contains('- INFO -').any():
        results['is_new_style'] = True
        # For new style, filter out ERROR lines for timestamp parsing
        log_series_for_timestamps = log_series[~log_series.str.contains('- ERROR -')]
    else:
        results['is_new_style'] = False
        log_series_for_timestamps = log_series


    # Define search strings based on log style
    search_strings = {
        True: {'start': 'Started: ', 'finished': 'Finished: '},
        False: {'start': 'Started ', 'finished': 'Finished '}
    }
    current_search_strings = search_strings[results['is_new_style']]

    # Extract Timestamps
    start_lines = log_series_for_timestamps[log_series_for_timestamps.str.contains(current_search_strings['start'])]
    finish_lines = log_series_for_timestamps[log_series_for_timestamps.str.contains(current_search_strings['finished'])]

    if not start_lines.empty:
        try:
            start_time_str = start_lines.str.split(current_search_strings['start']).map(
                lambda x: x[-1].strip()
            ).iloc[0]
            results['start_time'] = pd.to_datetime(start_time_str).isoformat()
        except Exception as e:
            logging.debug(f"Could not parse start time from {log_path}: {e}")
            results['error'] = results['error'] + "; Could not parse start time" if results['error'] else "Could not parse start time"


    if not finish_lines.empty:
        try:
            finish_time_str = finish_lines.str.split(current_search_strings['finished']).map(
                lambda x: x[-1].strip()
            ).iloc[0]
            results['end_time'] = pd.to_datetime(finish_time_str).isoformat()
        except Exception as e:
            logging.debug(f"Could not parse finish time from {log_path}: {e}")
            results['error'] = results['error'] + "; Could not parse finish time" if results['error'] else "Could not parse finish time"


    # Extract Branch and Commit details (using the original log_series before filtering errors)
    branch_lines = log_series[log_series.str.contains('Branch: ')] # More specific match
    commit_lines = log_series[log_series.str.contains('Commit: ')] # More specific match

    if not branch_lines.empty:
        try:
            results['branch'] = branch_lines.str.split(':').iloc[0][-1].strip()
        except IndexError:
            logging.debug(f"Could not parse branch from {log_path} - line format unexpected: {branch_lines.iloc[0]}")
            results['error'] = results['error'] + "; Branch parsing error" if results['error'] else "Branch parsing error"

    if not commit_lines.empty:
        try:
            results['commit'] = commit_lines.str.split(':').iloc[0][-1].strip()
        except IndexError:
            logging.debug(f"Could not parse commit from {log_path} - line format unexpected: {commit_lines.iloc[0]}")
            results['error'] = results['error'] + "; Commit parsing error" if results['error'] else "Commit parsing error"

    return results


def build_run_log_summary(root_dir: Path, m_out_path: Path, box_out_path: Optional[Path] = None) -> None:
    """
    Builds a DataFrame of run logs from a directory, processes them, and saves to CSV.

    Args:
        root_dir (Path): The path to the root directory containing run setup YAMLs.
        m_out_path (Path): The path to the output CSV file for M drive.
        box_out_path (Optional[Path]): The path to the output CSV file for Box.
    """
    run_data_records = []

    for yaml_path in root_dir.rglob("*run_setup*.yaml"):
        logging.info(f"Processing YAML: {yaml_path}")

        # Skip boilerplate/repo/history YAML files
        if yaml_path.name == 'run_setup.yaml' or \
           'bayarea_urbansim' in str(yaml_path.parent) or \
            'staging' in str(yaml_path.parent) or \
           '.history' in str(yaml_path.parent):
            logging.info(f"Skipping non-run YAML: {yaml_path}")
            continue

        run_info: Dict[str, Any] = {'yaml_path': str(yaml_path)}

        # Get YAML file creation/modification timestamp
        try:
            stat_info = yaml_path.stat()
            # Using ctime but could consider mtime.
            file_timestamp_epoch = getattr(stat_info, 'st_birthtime', stat_info.st_ctime)
            file_dt = datetime.datetime.fromtimestamp(file_timestamp_epoch)
            run_info['time_stamp_yaml_file'] = file_dt.isoformat()
        except Exception as e:
            logging.warning(f"Could not get file timestamp for {yaml_path}: {e}")
            run_info['time_stamp_yaml_file'] = None

        # Load data from YAML file
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                yaml_content = yaml.safe_load(f)
                if isinstance(yaml_content, dict):
                    run_info.update(yaml_content)
                else:
                    logging.warning(f"YAML content in {yaml_path} is not a dictionary. Skipping its content.")
                    run_info['yaml_load_error'] = "Content not a dictionary"
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file {yaml_path}: {e}")
            run_info['yaml_load_error'] = str(e)
            # Decide if you want to continue processing this entry or skip
            # For now, we'll add it with the error and None for log data

        # Get corresponding log file path
        log_file_path = get_log_file_path_from_yaml(yaml_path)
        run_info['log_file_path'] = str(log_file_path) # Store the expected path

        # Parse the log file
        log_data = parse_log_file(log_file_path)

        run_info['time_stamp_log_start'] = log_data['start_time']
        run_info['time_stamp_log_end'] = log_data['end_time']
        run_info['git_branch'] = log_data['branch']
        run_info['git_commit'] = log_data['commit']
        run_info['log_is_new_style'] = log_data['is_new_style']
        run_info['log_parsing_error'] = log_data['error']


        run_data_records.append(run_info)

    if not run_data_records:
        logging.info("No run records found. Check the root directory and file patterns.")
        return

    df = pd.DataFrame.from_records(run_data_records)

    # Add a column to flag whether the run was completed (based on end time from log)
    df['is_complete_from_log'] = df['time_stamp_log_end'].notna()

    # Reorder columns to put metadata first
    meta_cols = [
        'yaml_path', 'log_file_path', 'is_complete_from_log',
        'time_stamp_yaml_file', 'time_stamp_log_start', 'time_stamp_log_end',
        'git_branch', 'git_commit', 'log_is_new_style', 'log_parsing_error', 'yaml_load_error'
    ]
    
    # Ensure all meta_cols exist in df, add if missing (e.g. if all YAMLs failed to load)
    for col in meta_cols:
        if col not in df.columns:
            df[col] = None 

    existing_data_cols = [c for c in df.columns if c not in meta_cols]
    df = df[meta_cols + existing_data_cols]

    # df_completed = df[df['is_complete_from_log']].copy() # Use .copy() to avoid SettingWithCopyWarning

    # For now, let's save all records, including incomplete ones, as the 'is_complete_from_log' flags it.
    try:
        df.to_csv(m_out_path, index=False)
        logging.info(f"Successfully wrote run log summary to: {m_out_path}")
    except Exception as e:
        logging.error(f"Failed to write CSV to M drive {m_out_path}: {e}")

    if box_out_path:
        try:
            # Ensure parent directory exists for Box path
            box_out_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(box_out_path, index=False)
            logging.info(f"Successfully wrote run log summary to: {box_out_path}")
        except Exception as e:
            logging.error(f"Failed to write CSV to Box {box_out_path}: {e}")


if __name__ == "__main__":
    # Determine M_DRIVE based on OS - this is brittle
    if os.name == "nt":  # Windows
        M_DRIVE = pathlib.Path("M:/")
    else:  # macOS or Linux
        # More robustly check for common mount points if '/Volumes/Data/Models' is not always it
        osx_mount = pathlib.Path("/Volumes/Data/Models")
        if osx_mount.is_dir():
             M_DRIVE = osx_mount
        else:
            # Fallback or raise an error if M_DRIVE cannot be determined. Fix later
            logging.warning(f"{osx_mount} not found. Defaulting M_DRIVE to current user's home")
            M_DRIVE = pathlib.Path.home()


    HOME_DIR = pathlib.Path.home()

    # Determine BOX_DIR based on username or a more general location
    # Consider using environment variables for paths like BOX_DIR for flexibility
    user = os.getlogin()
    if user in ['lzorn', 'jahrenholtz', 'aolsen']: # list e-drive specific users
        BOX_DIR = pathlib.Path("E:/Box") if os.name == "nt" else HOME_DIR / "Box" # Adjust E: for non-Windows
        if os.name != "nt" and not (HOME_DIR / "Box").is_dir(): # If ~/Box doesn't exist for these users on non-Windows
             alt_box_path = pathlib.Path("/Volumes/Box") # Common alternative Box mount on macOS
             if alt_box_path.is_dir():
                 BOX_DIR = alt_box_path
             else:
                 logging.warning(f"Default Box path {HOME_DIR / 'Box'} not found for user {user}. Please verify BOX_DIR.")
    else: # Default for other users
        BOX_DIR = HOME_DIR / 'Box'
        if not BOX_DIR.is_dir():
            alt_box_path = pathlib.Path("/Volumes/Box")
            if alt_box_path.is_dir():
                BOX_DIR = alt_box_path
            else:
                logging.warning(f"Default Box path {BOX_DIR} not found. Please verify BOX_DIR.")


    # Define output paths both for M and for box
    m_output_dir = M_DRIVE / "urban_modeling" / "baus" / "PBA50Plus"
    m_output_file = m_output_dir / 'run_setup_tracker_autogen_v2.csv' 

    box_output_dir = BOX_DIR / "Modeling and Surveys" / "Urban Modeling" / \
                     "Bay Area UrbanSim" / "PBA50plus Meta"
    box_output_file = box_output_dir / 'run_setup_tracker_autogen_v2.csv' 

    # Define root directory for searching YAML files
    search_root_dir = M_DRIVE / "urban_modeling" / "baus" / "PBA50Plus"

    # Create output directories if they don't exist
    #m_output_dir.mkdir(parents=True, exist_ok=True)
    # box_output_dir.mkdir(parents=True, exist_ok=True) # Done in the function for Box

    logging.info(f"Starting run log processing. Root directory: {search_root_dir}")
    logging.info(f"M drive output path: {m_output_file}")
    if box_output_file:
        logging.info(f"Box output path: {box_output_file}")

    # Call the main function
    build_run_log_summary(search_root_dir, m_output_file, box_output_file)

    logging.info("Script finished.")