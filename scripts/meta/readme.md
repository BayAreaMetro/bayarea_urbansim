## Meta folder

This subfolder contains ancillary scripts that help with managing BAUS runs, focused on getting details from the setup configuration files stored in YAML format.

### Scripts

`compare_two_runs_yaml_setup_files.py`
    
This script compares two BAUS run setup YAML files and generates a DataFrame highlighting the differences between them.
    
    The script takes three arguments:
     -p1, --dict1_path: Path to the first YAML file.
     -p2, --dict2_path: Path to the second YAML file.
     -o, --output_path (optional): Path to save the output CSV file. 
     Defaults to a predefined location in the user's file system.

`create_run_log_from_yamls.py`
    
    This script builds a DataFrame of run logs by recursively searching a directory for YAML run setup files.
    
    It extracts key information from the YAML files themselves, along with timestamps 
    from file creation and log files (for start and finish times).
    The script also flags whether a given run was completed based on 
    the presence of an end time in the log file.
    The script takes no arguments, but relies on pre-defined paths 
    in the script itself.

### Use Cases

* Use `compare_two_runs_yaml_setup_files.py` to compare different BAUS run setups and identify specific configuration differences.
* Use `create_run_log_from_yamls.py` to generate a full run inventory of BAUS runs performed to date. This is a hack solution - the plan is to have a run action submit information to a database and have that be queryable over time so as to not have to rebuild a run log every time a new run is started.
