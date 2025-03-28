import pandas as pd
from pathlib import Path
import pathlib
import yaml
import datetime
import os
from typing import Optional


# Script recursively searches for yaml run setup files and extracts the key BAUS run setup information
# for post-hoc meta-log of runs. It prepares a csv including runtime data from the yamls themselves,
# along with timestamps from the file creation as well as from the log file (for start and finish), 
# to flag whether a given run was run to completion.


def get_log_file_from_yaml_path(p: Path) -> Path:
    """
    Constructs the path to the log file from a run setup YAML file path.

    This function assumes the log file shares the same stem as the YAML file, 
    but with a `.log` extension, and potentially without prefixes/suffixes 
    containing "run_setup".

    Args:
        p (Path): The path to the run setup YAML file.

    Returns:
        Path: The path to the corresponding log file.
    """

    # use the yaml file to create the path to the log file
    p_log_file =  f'{p.stem}.log'.replace('_run_setup', '').replace('run_setup_', '')
    p_log_path = p.parent /  p_log_file
    return p_log_path


def get_branch_details_from_run_log(p_log_path: Path) -> dict:
    """
    Parses a log file to get GitHub branch and commit details of a run.

    Args:
        p_log_path (Path): The path object to the log file.

    Returns:
        dict: A dictionary containing 'branch' and 'commit' information 
              or None if not found.
    """

    output = {'branch': None, 'commit': None}

    # open the file object
    fobj = open(p_log_path, "r", encoding='utf-8')

    # read the lines in the log
    log_file = fobj.readlines()

    # turn to pd.Series for easy searching for start and finish text markers

    log_file_s = pd.Series(log_file)
    fobj.close()
    log_file_branch = log_file_s[log_file_s.str.contains('Branch')]
    log_file_commit = log_file_s[log_file_s.str.contains('Commit')]
    
    log_file_branch = log_file_branch.str.split(':').iloc[0][1].strip()
    log_file_commit = log_file_commit.str.split(':').iloc[0][1].strip()
    
    output['branch'] = log_file_branch
    output['commit'] = log_file_commit
    return output


def get_timestamp_from_run_log(p_log_path: Path) -> dict:
    """
    Parses a log file to get timestamps of start and finish of a run.

    Args:
        p_log_path (Path): The path object to the log file.

    Returns:
        dict: A dictionary containing 'start' and 'finish' timestamps in ISO format 
              or None if not found.
    """

    output = {'start': None, 'finish': None}

    # Open the file object
    fobj = open(p_log_path, "r", encoding='utf-8')

    # Read the lines in the log
    log_file = fobj.readlines()

    # Turn to pd.Series for easy searching for start and finish text markers
    log_file_s = pd.Series(log_file)
    fobj.close()

    # Log start and finish strings differ between old and new version
    search_strings = {True: {'start': 'Started: ', 'finished': 'Finished: '},
                      False: {'start': 'Started ', 'finished': 'Finished '}}

    is_new_log_style = is_new_style_log(p_log_path)
    if is_new_log_style:
        # kick out error logs - we don't need the verbose detail
        log_file_s = log_file_s[~log_file_s.str.contains('- ERROR -')]

    # Pass the appropriate search string depending on the vintage of the log file style
    log_file_start = log_file_s[log_file_s.str.contains(
        search_strings[is_new_log_style]['start'])]
    log_file_finish = log_file_s[log_file_s.str.contains(
        search_strings[is_new_log_style]['finished'])]

    # If the started string is found in the log, get it:
    if len(log_file_start) > 0:
        start_time = log_file_start.str.split(search_strings[is_new_log_style]['start']).map(
            lambda x: x[-1].replace('\n', '')).map(pd.to_datetime)
        output['start'] = start_time.iloc[0].isoformat()

    # If the finished string is found in the log, get it:
    if len(log_file_finish) > 0:
        finish_time = log_file_finish.str.split(search_strings[is_new_log_style]['finished']).map(
            lambda x: x[-1].replace('\n', '')).map(pd.to_datetime)
        output['finish'] = finish_time.iloc[0].isoformat()

    # log_date_format = "%a %b %d %H:%M:%S %Y"
    # parsed_date = datetime.datetime.strptime(log_date_string, log_date_format)
    # return parsed_date.isoformat()
    return output

def is_new_style_log(p_log_path: Path) -> bool:
    
    # open the file object
    fobj = open(p_log_path, "r", encoding='utf-8')

    # read the lines in the log
    log_file = fobj.readlines()
    
    fobj.close()

    # turn to pd.Series for easy searching for start and finish text markers
    log_file_s = pd.Series(log_file)
    
    # Use the presence of the INFO marker as a litmus test for a log file from the logging module
    if log_file_s.str.contains('- INFO -').any():
        return True
    else:
        return False
    

def build_run_log(root_dir: Path, m_out_path: Path, box_out_path: Optional[Path] = None) -> pd.DataFrame:
    
    """
    Builds a DataFrame of run logs from a directory.

    Args:
        root_dir (Path): The path to the root directory containing run logs.
        m_out_path (Path: The path to the output CSV file for ML.
        box_out_path (Path): The path to the output CSV file for Box.

    Returns:
        pd.DataFrame: A DataFrame containing information about each run log.
    """

    dicts = []

    for p in root_dir.rglob("*run_setup*.yaml"):
        print(p)

        # we skip the occasional boilerplate repo run_setup.yaml without suffixes in the name as not being 
        # actual run logs, vs code save history copies, as well as any repo yaml
        if p.name == 'run_setup.yaml' or 'bayarea_urbansim' in str(p.parent) or '.history' in str(p.parent):
            print(f'Skipping {p}')
            continue

        # get file stats
        this_stat = p.stat()

        # keep timestamp
        file_birth_ts = this_stat.st_birthtime if 'st_birthtime' in this_stat else this_stat.st_ctime

        file_birth_dt = datetime.datetime.fromtimestamp(file_birth_ts)
        ts_iso = file_birth_dt.isoformat()

        with open(p, 'r') as f:
            this_dict = yaml.safe_load(f)

        # get log file from yaml path for start and end time
        try:
            p_log_path = get_log_file_from_yaml_path(p)

            # then get the time stamp from the log file
            ts_dict = get_timestamp_from_run_log(p_log_path)

            this_dict['time_stamp_start_log'] = ts_dict['start']
            this_dict['time_stamp_end_log'] = ts_dict['finish']

            print(ts_dict['start'])

            # also get github branch / commit details
            git_dict = get_branch_details_from_run_log(p_log_path)
            this_dict['git_branch'] = git_dict['branch']
            this_dict['git_commit'] = git_dict['commit']
            

        except (FileNotFoundError, ValueError) as error:
            print(error)
            print(f'{p_log_path} not found')
            ts_iso = None

        this_dict['yaml_path'] = p
        this_dict['time_stamp_start_pth'] = ts_iso

        dicts.append(this_dict)
    df = pd.DataFrame.from_records(dicts)

    
    if len(df)>0:
        

        # add a column to flag whether the run was completed from endtime stamp
        df['is_complete'] = df.time_stamp_end_log.notna()

        # filter out incomplete runs
        df = df[df.is_complete]

        # Put meta columns up front

        cols = ['yaml_path', 'is_complete', 'time_stamp_start_log',
                'time_stamp_start_pth', 'time_stamp_end_log']
        # ...keep the order of the rest of the columns
        df = df[cols + [c for c in df.columns if c not in cols]]

        # write the file to disk (box and M)
        df.to_csv(m_out_path, index=False)
        if box_out_path is not None:
            df.to_csv(box_out_path, index=False)
    else:
        print('No runs found - check your file system / input path')



if __name__ == "__main__":

    # PATHS 
    
    # set the path for M: drive
    # from OSX, M:/ may be mounted to /Volumes/Data/Models
    M_DRIVE = pathlib.Path("/Volumes/Data/Models") if os.name != "nt" else pathlib.Path("M:/")
    HOME_DIR = pathlib.Path.home()
    BOX_DIR = HOME_DIR / 'Library/CloudStorage/Box-Box' if not os.getlogin()=='lzorn' else Path("E:/Box")

    #output paths
    
    m_out_path = M_DRIVE / "urban_modeling" / \
        "baus" / "PBA50Plus" / 'run_setup_tracker_autogen.csv'
    
    box_out_path = BOX_DIR / "Modeling and Surveys" / "Urban Modeling" / \
        "Bay Area UrbanSim" / "PBA50plus Meta" / 'run_setup_tracker_autogen.csv'

    root_dir = M_DRIVE / "urban_modeling" / "baus" / "PBA50Plus" 


    # call the function
    #build_run_log(root_dir, m_out_path, box_out_path)
    build_run_log(root_dir, m_out_path)
