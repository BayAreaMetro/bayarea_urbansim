# ==============================
# ===== Affordable Metrics =====
# ==============================

import logging
import pandas as pd

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
                'deed_restricted_pct': dr_units_share}

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
    return result_dr_units_share