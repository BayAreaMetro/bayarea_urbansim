# ==============================
# ====== Diverse Metrics =======
# ==============================

import pandas as pd
import logging

def low_income_households_share(parcel_geog_summary_initial, parcel_geog_summary_final, modelrun_id, modelrun_alias, plan, output_path):
    """
    Calculate the share of households that are low-income in Transit-Rich Areas (TRA), High-Resource Areas (HRA), and both
    
    Parameters:
    - parcel_geog_summary_initial (pd.DataFrame): DataFrame with columns ['hra_id', 'tra_id', 'tothh', 'hhq1'] for the initial year.
    - parcel_geog_summary_final (pd.DataFrame): DataFrame with columns ['hra_id', 'tra_id', 'tothh', 'hhq1'] for the final year.
    - modelrun_id (str): Unique identifier for the model run.
    - model_run_alias (str): An alias name for the model run.
    - plan (str): Indicates the plan type ('pba50' or 'pba50plus').
    - output_path (str or Path): The directory path to save the output CSV file.
    
    Returns:
    - result_hh_share: pd.DataFrame
        DataFrame containing the share of low-income households at the regional level and for TRAs, HRAs, and both.
    """ 
    
    def calculate_share_low_inc(df, area, year):
        total_hhs = df['tothh'].sum()
        low_inc_hhs = df['hhq1'].sum()
        logging.info(f"Calculating low-income household share for {area} in {year}: Total households - {total_hhs}, Low-income households - {low_inc_hhs}")
        low_inc_hh_share = round((low_inc_hhs / total_hhs), 3) if total_hhs > 0 else 0
        return {'modelrun_id': modelrun_id,
                'modelrun_alias': f"{year} {modelrun_alias}",
                'area': area,
                'Q1HH_share': low_inc_hh_share}
    
    results = []

    # Define the years to process
    years_data = {'initial': (parcel_geog_summary_initial, '2015' if plan == "pba50" else '2020'),
                  'final': (parcel_geog_summary_final, '2050')}

    # Define area filters
    if plan == "pba50":
        area_filters = {'HRA': lambda df: df['hra_id'] == 1,
                        'TRA': lambda df: df['tra_id'] == 1,
                        'HRATRA': lambda df: (df['tra_id'] == 1) & (df['hra_id'] == 1),
                        'EPC': lambda df: df['coc_flag_pba2050'] == 1,
                        'Region': None}
    elif plan == "pba50plus":
        area_filters = {'HRA': 'hra_id',
                        'TRA': lambda df: df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']),
                        'HRATRA': lambda df: (df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])) & (df['hra_id'] == 'HRA'),
                        'EPC': 'epc_id',
                        'Region': None}

    # Process each area and year
    for year_key, (df, year) in years_data.items():
        for area, filter_condition in area_filters.items():
            if filter_condition is not None:
                if callable(filter_condition):  # Check if the filter is a function
                    df_area = df[filter_condition(df)]
                else:
                    df_area = df[df[filter_condition] == area]
            else:
                df_area = df

            # Log the filtered dataframe
            logging.info(f"Filtering for {area} in {year}, number of rows: {len(df_area)}")
            if df_area.empty:
                logging.warning(f"No data found for {area} in {year}. This will result in 0 values for low-income household share.")
            
            # Calculate the share and append to results
            area_result = calculate_share_low_inc(df_area, area, year)
            results.append(area_result)

    # Create the results DataFrame
    result_hh_share = pd.DataFrame(results)

    result_hh_share['metric_type'] = 'diverse'
    return result_hh_share