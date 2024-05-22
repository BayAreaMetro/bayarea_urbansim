#!/usr/bin/env python

import argparse
import os
import pathlib
import yaml
import pandas as pd


def compare_yaml_dicts_to_dataframe(dict1_path, dict2_path, output_path):
    """
    Simple script to compare two BAUS YAML run setup files and generates a DataFrame highlighting the differences.

    Args:
      dict1_path (str): Path to the first YAML file.
      dict2_path (str): Path to the second YAML file.
      output_path (str): Path to save the output DataFrame as a CSV file.

    Returns:
      pd.DataFrame: DataFrame containing the keys and values from both dictionaries,
          highlighting mismatched values between them.
    """

    # If pathlib.Path object (likely) not passed, turn into one from string

    dict1_path = pathlib.Path(dict1_path) if type(
        dict1_path) != pathlib.PosixPath else dict1_path

    dict2_path = pathlib.Path(dict2_path) if type(
        dict2_path) != pathlib.PosixPath else dict2_path


    # Open the yamls
    with open(dict1_path) as f:
        dict1 = yaml.safe_load(f)
    with open(dict2_path) as f:
        dict2 = yaml.safe_load(f)

    # Get a record for each key, regardless of origin
    df = pd.concat([pd.Series(dict1), pd.Series(dict2)], keys=[
                   'reference_run', 'subject_run'], axis=1)
    df.index = df.index.set_names('key')

    # Fill missing values with 'NOT USED'
    df.fillna('NOT USED', inplace=True)

    # Filter differences
    df = df[df['reference_run'] != df['subject_run']].reset_index()

    # Generate output filename
    output_file = pathlib.Path(f'setup_diff_{dict1_path.stem}_VS_{dict2_path.stem}.csv'.replace(
        'run_setup_', ''))

    # Save DataFrame as CSV
    df.to_csv(output_path / output_file)

    return df



if __name__ == '__main__':

    # Handle OS M mount (AO) vs windows m-drive location
    m_drive = pathlib.Path(
    "/Volumes/Data/Models") if os.name != "nt" else pathlib.Path("M:")

    # set a default output dir
    output_path = pathlib.Path(m_drive,'urban_modeling' , 'baus' , 'PBA50Plus','diffs' )


    parser = argparse.ArgumentParser(
        description='Compare two BAUS run_setup.yaml files and generate a diff DataFrame as CSV.')
    parser.add_argument('-p1','--dict1_path', type=str,
                        help='Path to the first YAML file.')
    parser.add_argument('-p2','--dict2_path', type=str,
                        help='Path to the second YAML file.')
    parser.add_argument('-o','--output_path', type=str, default=output_path,
                        help='Path to save the output CSV file.')

    args = parser.parse_args()

    print(args.output_path)

    compare_yaml_dicts_to_dataframe(
        args.dict1_path, args.dict2_path, args.output_path)
    
    # paths and usage, e.g. 
    # yaml_1 = '/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/PBA50Plus_DraftBlueprint/PBA50Plus_Draft_Blueprint_v8_znupd_nodevfix/run_setup_PBA50Plus_Draft_Blueprint_v8_znupd_nodevfix.yaml'
    # yaml_2 = '/Volumes/Data/Models/urban_modeling/baus/PBA50Plus/PBA50Plus_DraftBlueprint/PBA50Plus_Draft_Blueprint_v8_znupd_nodevfix_altseed_v2/run_setup_PBA50Plus_Draft_Blueprint_v8_znupd_nodevfix_altseed_v2.yaml'
    # compare_yaml_dicts_to_dataframe(yaml_1,yaml_2, output_path)

    print(
        f'YAML comparison results saved to: {args.output_path}')