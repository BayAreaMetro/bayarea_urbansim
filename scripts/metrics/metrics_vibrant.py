# ==============================
# ====== Vibrant Metrics =======
# ==============================

import pandas as pd

def jobs_housing_ratio(county_summary_initial, county_summary_final, modelrun_id, modelrun_alias, plan, output_path):
    """
    Calculate and export the jobs-to-housing ratio for the initial and final years for each county and the whole region.
    
    Parameters:
    - county_summary_initial: pd.DataFrame with columns ['county', 'totemp', 'residential_units'] for the initial year.
    - county_summary_final: pd.DataFrame with columns ['county', 'totemp', 'residential_units'] for the final year.
    - modelrun_id: str, the unique identifier for the model run.
    - modelrun_alias: str, a friendly name for the model run.
    - plan: str, indicates the plan type ('pba50' or 'pba50plus').
    - output_path: str or Path, the directory path to save the output CSV file.
    
    Returns:
    - pd.DataFrame with the jobs-to-housing ratio for each county and the whole region for the initial and final plan years.
    """
    column_mapping = {"pba50": ('TOTEMP', 'TOTHH', 'COUNTY_NAME'), #'RES_UNITS'
                      "pba50plus": ('totemp', 'tothh', 'county')} #'residential_units'
        
    total_job_column, res_unit_column, county_column = column_mapping.get(plan.lower(), column_mapping['pba50plus']) 
    
    def calculate_ratio(df, year):
        # Group by county and calculate jobs-housing ratio for each county
        df_grouped = df.groupby(county_column).apply(lambda x: round(x[total_job_column].sum() / x[res_unit_column].sum(), 3) if x[res_unit_column].sum() > 0 else 0).reset_index(name='jobs_housing_ratio')
        # Add a row for the overall region
        df_grouped.loc[len(df_grouped)] = ["Regionwide", round(df[total_job_column].sum() / df[res_unit_column].sum(), 3)]
        df_grouped['year'] = year
        df_grouped['modelrun_id'] = modelrun_id
        df_grouped['modelrun_alias'] = f"{year} {modelrun_alias}"
        
        return df_grouped
    
    # Calculate ratios for initial and final years
    jh_ratios_initial = calculate_ratio(county_summary_initial, 2015 if plan == "pba50" else 2020)
    jh_ratios_final = calculate_ratio(county_summary_final, 2050)

    # Combine the initial and final ratios into one DataFrame
    combined_jh_ratios_df = pd.concat([jh_ratios_initial, jh_ratios_final], ignore_index=True)
    
    # Rename the county column to 'county' regardless of the original name
    combined_jh_ratios_df.rename(columns={county_column: 'county'}, inplace=True)

    # Select and order the columns  
    combined_jh_ratios_df = combined_jh_ratios_df[['modelrun_id', 'modelrun_alias', 'county', 'jobs_housing_ratio']]

    combined_jh_ratios_df['metric_type'] = 'vibrant'
    return combined_jh_ratios_df