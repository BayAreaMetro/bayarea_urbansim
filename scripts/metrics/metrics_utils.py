import pandas as pd
import logging
from datetime import datetime

# --------------------------------------
# Data Loading Based on Model Run Plan
# --------------------------------------
def load_data_for_runs(core_summaries_paths, geographic_summaries_paths, plan):
    """
    Loads core and geographic summary data for various BAUS model runs. This function searches for files matching patterns 
    indicating summary data for the years specific to PBA50Plus or PBA50 runs, loads them into DataFrames, and aggregates these
    DataFrames into two lists: one for core summaries and one for geographic summaries.

    Parameters:
    - core_summaries_paths (list of Path objects): Paths to directories containing core summary files.
    - geographic_summaries_paths (list of Path objects): Paths to directories containing geographic summary files.
    - plan (str): Indicates the plan name, affecting which file name patterns to use.

    Returns:
    - tuple of two lists: 
        - The first list contains DataFrames loaded from core summary files.
        - The second list contains DataFrames loaded from geographic summary files.
    
    Both lists include DataFrames for files that match the year-specific patterns, assuming files for both target years are present.
    """
    core_summary_dfs = []
    geographic_summary_dfs = []

    # Patterns to match files ending with the specified years
    if plan == "pba50plus":
        core_summary_patterns = ["*_parcel_summary_2020.csv", "*_parcel_summary_2050.csv"]
        geo_summary_patterns = ["*_county_summary_2020.csv", "*_county_summary_2050.csv"]
    elif plan == "pba50":
        core_summary_patterns = ["*_parcel_data_2015.csv", "*_parcel_data_2050.csv"]
        geo_summary_patterns = ["*_county_summaries_2015.csv", "*_county_summaries_2050.csv"]
    else:
        raise ValueError(f"Unrecognized plan: {plan}")
    
    # Load core summaries if files matching both patterns exist
    for core_path in core_summaries_paths:
        matching_files = [list(core_path.glob(pattern)) for pattern in core_summary_patterns]
        if all(matching_files):  # Check if there are files for both patterns
            for file_list in matching_files:
                for file in file_list:
                    df = pd.read_csv(file) # consider adding usecols when all metrics are defined to increase the speed
                    logging.info(f"Loaded file {file} with shape {df.shape}")
                    core_summary_dfs.append(df)

    # Load geographic summaries if files matching both patterns exist
    for geo_path in geographic_summaries_paths:
        matching_files = [list(geo_path.glob(pattern)) for pattern in geo_summary_patterns]
        if all(matching_files):  
            for file_list in matching_files:
                for file in file_list:
                    df = pd.read_csv(file)
                    logging.info(f"Loaded file {file} with shape {df.shape}")
                    geographic_summary_dfs.append(df)

    return core_summary_dfs, geographic_summary_dfs

# -------------------------
# Crosswalk Data Handling
# -------------------------
def load_crosswalk_data(crosswalk_path):
    """
    This function loads the crosswalk data called parcels_geography from the BAUS Inputs folder. This dataset map parcel IDs to other geographic identifiers, 
    facilitating the enrichment of parcel summary data with additional geographic information.

    Parameters: 
    - crosswalk_path (str or Path): The file path to the most recent parcels_geography CSV file.

    Returns:
    - pandas.DataFrame: A DataFrame containing the loaded crosswalk data.

    """
    parcels_geography = pd.read_csv(crosswalk_path, na_values="NA", low_memory=False)
    return parcels_geography

# -------------------------------------------
# Merging Model Run dfs with Crosswalk Data
# ----------------------    -----------------
# This function should be called after loading each run's data and before calculating metrics.
def merge_with_crosswalk(parcel_summary_dfs, parcels_geography):
    """
    This function merges each DataFrame in a list of parcel summary DataFrames with the parcels geography DataFrame on parcel ID values. 

    Parameters: 
    - parcel_summary_dfs (list of pandas.DataFrame): List of DataFrames containing parcel summary data.
    - parcels_geography (pandas.DataFrame): DataFrame containing the crosswalk data mapping parcel IDs to parcel summary data.

    Returns:
    - list of pandas.DataFrame: A list of DataFrames, each showing a parcel summary DataFrame merged with the parcels_geography data.
    """
    merged_dfs = []
    for df in parcel_summary_dfs:
        merged_df = df.merge(parcels_geography, left_on="parcel_id", right_on="PARCEL_ID", how="left")
        merged_dfs.append(merged_df)
    return merged_dfs

# -----------------------------------
# Identifier Extraction from Path
# -----------------------------------
def extract_ids_from_row(row):
    """
    Extracts the model run alias and model run identifier from a DataFrame row.
    
    Parameters:
    - row (pd.Series): A row from the df_model_runs DataFrame, containing at least 'scenario_group' and 'directory' columns.
    
    Returns:
    - tuple: A tuple containing two strings: (modelrun_alias, modelrun_id), where modelrun_alias is derived from the 'scenario_group' 
            column, and modelrun_id is extracted from the 'directory' column, following the specified path structure.
    """
    modelrun_alias = row['scenario_group']
    
    base_path = "C:/Users/nrezaei/Box/Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/PBA50/"
    modelrun_id = row['directory'].replace(base_path.format("{}"), "").replace("\\", "/")
    
    return modelrun_alias, modelrun_id

# -----------------------------------
# Metrics Results Saving Utility
# -----------------------------------
def save_metric_results(df, metric_name, modelrun_id, modelrun_alias, output_path):
    filename = f"{metric_name}_{modelrun_id}_{modelrun_alias}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
    filepath = output_path / "Metrics" / filename
    df.to_csv(filepath, index=False)
    logging.info(f"Saved {metric_name} results to {filepath}")

# ----------------------------------
# Results Assembly for the Metrics
# ----------------------------------
def assemble_results_wide_format(modelrun_id, modelrun_alias, metrics_data):
    """
    Assemble metric results into a DataFrame with a wide format.

    Parameters:
    - modelrun_id: str, identifier for the model run.
    - modelrun_alias: str, a friendly or alias name for the model run.
    - metrics_data: dict, a dictionary where each key is a metric name and each value is a tuple or list containing:
        - A list of years (int)
        - A list of corresponding metric values (float or int)
    
    Returns:
    - pd.DataFrame with the assembled metric results in a wide format, where each metric has its own column,
      including 'modelrun_id', 'modelrun_alias', and 'year' as separate columns.
    """
    # Initialize an empty list to hold each row of our resulting DataFrame
    results_list = []

    # Iterate through each metric in the metrics_data
    for metric_name, (years, values) in metrics_data.items():
        # Assuming each metric has values for the same set of years, 
        # this loop will create a row for each year with the metric's value
        for year, value in zip(years, values):
            result = {
                "year": year,
                metric_name: value  # Dynamically name the column after the metric
            }
            results_list.append(result)
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(results_list)

    # Since multiple metrics can share the same year, we need to group by 'year' before pivoting
    # This step aggregates all metric values for the same year into a single row
    df_grouped = df.groupby('year', as_index=False).first()

    # Add 'modelrun_id' and 'modelrun_alias' to the DataFrame
    df_grouped['modelrun_id'] = modelrun_id
    # Concatenate 'year' and 'modelrun_alias'
    df_grouped['modelrun_alias'] = df_grouped['year'].astype(str) + " " + modelrun_alias
    
    # Drop the 'year' column as it's now redundant
    df_grouped = df_grouped.drop('year', axis=1)

    # Reorder columns to place 'modelrun_id', 'modelrun_alias' at the front
    cols = ['modelrun_id', 'modelrun_alias'] + [col for col in df_grouped if col not in ['modelrun_id', 'modelrun_alias']]
    df_wide = df_grouped[cols]
    
    return df_wide

# -----------------------------------
# Extract Concatenated String Values
# -----------------------------------

def extract_pba50_concat_values(row):
    """
    Extracts values from concatenated strings within a DataFrame row.

    The function identifies specific segments within the string, denoted by
    'GG', 'tra', 'HRA', 'DIS'. Each segment corresponds to a particular ID and 
    is assigned a value of 1 if present.

    Parameters:
    - row (str): A single string from the DataFrame's row.

    Returns:
    - pd.Series: A Series object containing the extracted values for 'gg_id', 
                 'tra_id', 'hra_id', and 'dis_id'.
    """
     # Initialize the id values with 0
    gg_id, tra_id, hra_id, dis_id = 0, 0, 0, 0
    
    # Return all 0s if the input is not a string or represents missing data
    if not isinstance(row, str) or row == 'nan':
        return pd.Series([gg_id, tra_id, hra_id, dis_id], index=['gg_id', 'tra_id', 'hra_id', 'dis_id'])
    
    # Special handling for 'eirzoningmodcat' format: remove city name and anything before the first 'NA', 'GG', or any capitalized letter
    if any(char.isupper() for char in row[:3]):  # Assumes city names do not start with uppercase in the first 3 chars
        first_capitalized = next((i for i, char in enumerate(row) if char.isupper()), None)
        row = row[first_capitalized:]
        
    # Set to 1 if the respective segment is present in the string
    if row.startswith('GG'):
        gg_id = 1
        row = row[2:]  # Process the remaining string
    
    if 'tra' in row:
        tra_id = 1
        row = row[row.index('tra')+3:]  # Process the remaining string

    if 'HRA' in row:
        hra_id = 1
        row = row[row.index('HRA')+3:]  # Process the remaining string

    if 'DIS' in row:
        dis_id = 1
        row = row[row.index('DIS')+3:]  # Process the remaining string

    return pd.Series([gg_id, tra_id, hra_id, dis_id], index=['gg_id', 'tra_id', 'hra_id', 'dis_id'])
