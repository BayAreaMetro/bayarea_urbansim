# ==============================
# ===== Affordable Metrics =====
# ==============================

import logging
import pandas as pd
import numpy as np
from metrics_utils import map_area_to_alias 

# Hard-coded numbers for off-model units from plan PBA50. Mark: We no longer use them for PBA50Plus metric calculations because:
        # Preserved is integrated into the model input provided for H2 (i.e., the preserved units targets by geography combination) 
        # Homeless is integrated into the model input provided for H4, similar to H2 above. 
        # It is assumed that 'pipeline' units are already incorporated in the pipeline data.

#offmodel_preserved = 40000
#offmodel_homeless = 35000
#offmodel_pipeline = 7572

def deed_restricted_affordable_share(
        parcel_geog_summary_initial: pd.DataFrame,
        parcel_geog_summary_final: pd.DataFrame,
        modelrun_id: str,
        modelrun_alias: str,
        plan: str,
        output_path: str
        ) -> pd.DataFrame:
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
    - result_dr_share (pd.DataFrame): DataFrame containing the share of housing that is deed-restricted affordable,
        at the regional level and for EPCs and HRAs.
    """

    # Define area filters
    if plan == "pba50":
        area_filters = {'HRA': lambda df: df['hra_id'] == 1,
                        'EPC': lambda df: df['coc_flag_pba2050'] == 1,
                        'Region': None}
    elif plan == "pba50plus":
        area_filters = {'HRA': lambda df: df['hra_id'] == 'HRA',
                        'EPC': lambda df: df['epc_id'] == 'EPC',
                        'Region': None}
    
    def calculate_share_dr_units(df, area, year):
        # Initialize variables
        dr_units = res_units = 0
        
        # Apply specific logic based on area
        if 'deed_restricted_units' in df.columns:
            if area == 'Region':
                if year == '2050':
                    dr_units = df['deed_restricted_units'].sum() #+ offmodel_preserved + offmodel_homeless + offmodel_pipeline
                    res_units = df['residential_units'].sum() #+ offmodel_homeless
                else:
                    dr_units = df['deed_restricted_units'].sum() - df['preserved_units'].sum()
                    res_units = df['residential_units'].sum()
            elif area == 'HRA':
                if year == '2050':
                    dr_units = df['deed_restricted_units'].sum()
                    res_units = df['residential_units'].sum()
                else:
                    dr_units = df['deed_restricted_units'].sum() - df['preserved_units'].sum()
                    res_units = df['residential_units'].sum()
            elif area == 'EPC':
                if year == '2050':
                    dr_units = df['deed_restricted_units'].sum() #+ offmodel_preserved
                    res_units = df['residential_units'].sum()
                else:
                    dr_units = df['deed_restricted_units'].sum() - df['preserved_units'].sum()
                    res_units = df['residential_units'].sum()
        else:
            logging.warning(f"DataFrame for {area} in {year} does not contain 'deed_restricted_units'.")
       
        # Calculate the share
        dr_units_share = round((dr_units / res_units), 3) if res_units > 0 else 0
        
        return {'modelrun_id': modelrun_id,
                'modelrun_alias': f"{year} {modelrun_alias}",
                'area_alias': map_area_to_alias(area),
                'area': area,
                'deed_restricted_pct': dr_units_share,
                'metric_name': 'deed_restricted_affordable_share'}

    results = []
    years_data = {'initial': '2015' if plan == 'pba50' else '2020', 'final': '2050'}

    for year_key, year in years_data.items():
            df_to_use = parcel_geog_summary_initial if year_key == 'initial' else parcel_geog_summary_final
            for area, filter_condition in area_filters.items():
                if callable(filter_condition):
                    df_area = df_to_use.loc[filter_condition(df_to_use)]
                elif filter_condition is not None:
                    condition = df_to_use[filter_condition] == area
                    df_area = df_to_use[condition]
                else:
                    df_area = df_to_use
        
                if not isinstance(df_area, pd.DataFrame):
                    logging.error(f"Expected df_area to be a DataFrame but got {type(df_area)}. Check the filtering logic.")
                    continue  
        
                # Calculate the share and append to results
                area_result = calculate_share_dr_units(df_area, area, year)
                results.append(area_result)

    # Create the results DataFrame
    result_dr_units_share = pd.DataFrame(results)
    result_dr_units_share['metric_type'] = 'affordable'
        
    return result_dr_units_share


def new_prod_deed_restricted_affordable_share(
        parcel_geog_summary_initial: pd.DataFrame,
        parcel_geog_summary_final: pd.DataFrame,
        modelrun_id: str,
        modelrun_alias: str,
        plan: str,
        output_path: str
    ) -> pd.DataFrame:
    """
    Calculate the share of new housing production that is deed-restricted affordable between initial and final year of the plan
    at the regional level, within Equity Priority Communities (EPC), and within High-Resource Areas (HRA)
    
    Parameters:
    - parcel_geog_summary_2020 (pd.DataFrame): DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    - parcel_geog_summary_2050 (pd.DataFrame): DataFrame with columns ['hra_id', 'epc_id', 'parcel_id', 'residential_units', 'deed_restricted_units'].
    
    Returns:
    - result_dr_share (pd.DataFrame): DataFrame containing the share of new housing production that is deed-restricted affordable,
        at the regional level and for EPCs and HRAs.
    """
    # Define area filters
    if plan == "pba50":
        area_filters = {'HRA': lambda df: df['hra_id'] == 1,
                        'EPC': lambda df: df['coc_flag_pba2050'] == 1,
                        'Region': None}
    elif plan == "pba50plus":
        area_filters = {'HRA': lambda df: df['hra_id'] == 'HRA',
                        'EPC': lambda df: df['epc_id'] == 'EPC',
                        'Region': None}
        
    def calculate_share_new_dr_units(df_initial, df_final, area, year='2050'):
        if area == 'Region':
            preserved_units_initial = 0
            preserved_units_final = df_final['preserved_units'].sum() #+ offmodel_preserved
            deed_restricted_units_initial = df_initial['deed_restricted_units'].sum() - df_initial['preserved_units'].sum()
            deed_restricted_units_final = df_final['deed_restricted_units'].sum() #+ offmodel_preserved + offmodel_homeless + offmodel_pipeline
            total_units_initial = df_initial['residential_units'].sum()
            total_units_final = df_final['residential_units'].sum() #+ offmodel_homeless
        elif area == 'HRA':
            preserved_units_initial = 0
            preserved_units_final = df_final['preserved_units'].sum()
            deed_restricted_units_initial = df_initial['deed_restricted_units'].sum() - df_initial['preserved_units'].sum()
            deed_restricted_units_final = df_final['deed_restricted_units'].sum() 
            total_units_initial = df_initial['residential_units'].sum()
            total_units_final = df_final['residential_units'].sum()
        elif area == 'EPC':
            preserved_units_initial = 0
            preserved_units_final = df_final['preserved_units'].sum() #+ offmodel_preserved
            deed_restricted_units_initial = df_initial['deed_restricted_units'].sum() - df_initial['preserved_units'].sum()
            deed_restricted_units_final = df_final['deed_restricted_units'].sum() #+ offmodel_preserved
            total_units_initial = df_initial['residential_units'].sum()
            total_units_final = df_final['residential_units'].sum()
        
        # Calculate differences and share
        residential_units_diff = total_units_final - total_units_initial
        deed_restricted_prod_total_initial = deed_restricted_units_initial - preserved_units_initial
        deed_restricted_prod_total_final = deed_restricted_units_final - preserved_units_final 
        deed_restricted_diff = deed_restricted_prod_total_final - deed_restricted_prod_total_initial
        new_dr_units_share = round((deed_restricted_diff / residential_units_diff), 3) if residential_units_diff > 0 else 0
    
        return {'modelrun_id': modelrun_id,
                'modelrun_alias': f"{year} {modelrun_alias}",
                'area_alias': map_area_to_alias(area),
                'area': area,
                'deed_restricted_pct_newUnits': new_dr_units_share,
                'metric_name': 'new_prod_deed_restricted_affordable_share'}
    
    results = []
    
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
    
    return result_new_dr_units_share 


def at_risk_housing_preserv_share(
        modelrun_id: str,
        modelrun_alias: str,
        output_path: str
    ) -> pd.DataFrame:
    """
    Creates a DataFrame that indicates the percentage of at-risk preservation for a specific model run.
    Depending on the model run alias, this function will assign a value to the 'at_risk_preserv_pct' field:
    1 if the model run alias is "No Project", otherwise 0. 

    Parameters:
    - modelrun_id (str): The identifier for the model run.
    - modelrun_alias (str): The alias for the model run.
    - output_path (str): The file path where the resulting DataFrame will be saved.
    
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
