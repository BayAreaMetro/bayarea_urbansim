# ==============================
# ===== Affordable Metrics =====
# ==============================

import logging
import pandas as pd
from metrics_utils import map_area_to_alias 

def deed_restricted_affordable_share(parcel_geog_summary_initial, parcel_geog_summary_final, modelrun_id, modelrun_alias, plan, output_path):
    """
    Calculate the share of housing that is deed-restricted affordable at the regional level, 
    within Equity Priority Communities (EPC), and within High-Resource Areas (HRA)
    
    Parameters:
    - parcel_geog_summary_initial (pd.DataFrame): DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    - parcel_geog_summary_final (pd.DataFrame): DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    - modelrun_id (str): The unique identifier for the model run.
    - modelrun_alias (str): An alias name for the model run.
    - plan (str): Indicates the plan type.
    - output_path (str or Path): The directory path to save the output CSV file.
    
    Returns:
    - result_dr_share: pd.DataFrame
        DataFrame containing the share of housing that is deed-restricted affordable,
        at the regional level and for EPCs and HRAs.
    """
    def calculate_share_dr_units(df, area, year):
        total_units = df['residential_units'].sum()
        logging.info(f"Calculating deed-restricted affordable share for {area} in {year}: Total units - {total_units}")
        deed_restricted_units = df['deed_restricted_units'].sum()
        dr_units_share = round((deed_restricted_units / total_units), 3) if total_units > 0 else 0

        return {'modelrun_id': modelrun_id,
                'modelrun_alias': f"{year} {modelrun_alias}",
                'area': area,
                'deed_restricted_pct': dr_units_share,
                'metric_name': 'deed_restricted_affordable_share'}

    results = []
    years_data = {'initial': (parcel_geog_summary_initial, '2015' if plan == 'pba50' else '2020'),
                  'final': (parcel_geog_summary_final, '2050')}

    # Define area filters
    if plan == "pba50":
        area_filters = {'HRA': lambda df: df['hra_id'] == 1,
                        'EPC': lambda df: df['coc_flag_pba2050'] == 1,
                        'Region': None}
    elif plan == "pba50plus":
        area_filters = {'HRA': 'hra_id',
                        'EPC': 'epc_id',
                        'Region': None}
    
    for year_key, (df, year) in years_data.items():
        for area, filter_condition in area_filters.items():
            if filter_condition is not None:
                if callable(filter_condition):  # Check if the filter is a function
                    df_area = df[filter_condition(df)]
                else:
                    df_area = df[df[filter_condition] == area]
            else:
                df_area = df
            
            # Calculate the share and append to results
            area_result = calculate_share_dr_units(df_area, area, year)
            results.append(area_result)

    # Create the results DataFrame
    result_dr_units_share = pd.DataFrame(results)

    result_dr_units_share['metric_type'] = 'affordable'
    result_dr_units_share['area_alias'] = result_dr_units_share['area'].apply(map_area_to_alias)

    return result_dr_units_share


def new_prod_deed_restricted_affordable_share(parcel_geog_summary_initial, parcel_geog_summary_final, modelrun_id, modelrun_alias, plan, output_path):
    """
    Calculate the share of new housing production that is deed-restricted affordable between initial and final year of the plan
    at the regional level, within Equity Priority Communities (EPC), and within High-Resource Areas (HRA)
    
    Parameters:
    - parcel_geog_summary_2020: pd.DataFrame
        DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    - parcel_geog_summary_2050: pd.DataFrame
        DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    
    Returns:
    - result_dr_share: pd.DataFrame
        DataFrame containing the share of new housing production that is deed-restricted affordable,
        at the regional level and for EPCs and HRAs.
    """
    def calculate_share_new_dr_units(df_initial, df_final, area):
        total_units_initial = df_initial['residential_units'].sum()
        total_units_final = df_final['residential_units'].sum()
        deed_restricted_units_initial = df_initial['deed_restricted_units'].sum()
        deed_restricted_units_final = df_final['deed_restricted_units'].sum()
        preserved_units_intial = df_initial['preserved_units'].sum()
        preserved_units_final = df_final['preserved_units'].sum()

        total_increase = total_units_final - total_units_initial
        deed_restricted_prod_total_initial = deed_restricted_units_initial - preserved_units_intial
        deed_restricted_prod_total_final = deed_restricted_units_final - preserved_units_final
        deed_restricted_new_prod = deed_restricted_prod_total_final - deed_restricted_prod_total_initial

        new_dr_units_share = round((deed_restricted_new_prod / total_increase), 3) if total_increase > 0 else 0
        return {'modelrun_id': modelrun_id,
                'modelrun_alias': f"{2050} {modelrun_alias}",
                'area': area,
                'deed_restricted_pct_newUnits': new_dr_units_share,
                'metric_name': 'new_prod_deed_restricted_affordable_share'}

    results = []

    # Define area filters
    if plan == "pba50":
        area_filters = {'HRA': lambda df: df['hra_id'] == 1,
                        'EPC': lambda df: df['coc_flag_pba2050'] == 1,
                        'Region': None}
    elif plan == "pba50plus":
        area_filters = {'HRA': lambda df: df['hra_id'] == 'HRA',
                        'EPC': lambda df: df['epc_id'] == 'EPC',
                        'Region': None}
    
    for area, filter_condition in area_filters.items():
        df_initial = parcel_geog_summary_initial
        df_final = parcel_geog_summary_final
        if filter_condition is not None:
            # If filter_condition is callable, apply it directly
            if callable(filter_condition):
                df_initial = df_initial[filter_condition(df_initial)]
                df_final = df_final[filter_condition(df_final)]
            else:
                logging.warning(f"Filter condition for {area} is not callable. Skipping filter.")
        area_result = calculate_share_new_dr_units(df_initial, df_final, area)
        results.append(area_result)

    # Create the results DataFrame
    result_new_dr_units_share = pd.DataFrame(results)
    result_new_dr_units_share['metric_type'] = 'affordable_new_prod'
    result_new_dr_units_share['area_alias'] = result_new_dr_units_share['area'].apply(map_area_to_alias)
    
    return result_new_dr_units_share 


def at_risk_housing_preserv_share(modelrun_id, modelrun_alias, output_path):
    """
    Creates a DataFrame that indicates the percentage of at-risk preservation for a specific model run.
    Depending on the model run alias, this function will assign a value to the 'at_risk_preserv_pct' field:
    1 if the model run alias is "No Project", otherwise 0. 

    Parameters:
    - modelrun_id (str): The identifier for the model run.
    - modelrun_alias (str): The alias for the model run, which dictates the value assigned to 'at_risk_preserv_pct'.
    - output_path (str): The file path where the resulting DataFrame may be saved (currently unused in the function).
    
    Returns:
    - pd.DataFrame: A DataFrame with the columns ['modelrun_id', 'modelrun_alias', 'area_alias', 'at_risk_preserv_pct', 'metric_type'].
    """
    results = []
    value = 0 if modelrun_alias in ["No Project"] else 1

    results.append({'modelrun_id': modelrun_id,
                    'modelrun_alias': f"2050 {modelrun_alias}",
                    'area_alias': 'Regionwide',
                    'at_risk_preserv_pct': value})
    
    # Convert the list of dictionaries into a pandas DataFrame
    at_risk_preserved_share = pd.DataFrame(results)
    at_risk_preserved_share['metric_type'] = 'affordable_at_risk_preserv'

    return at_risk_preserved_share
