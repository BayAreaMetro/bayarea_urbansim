# ==============================
# ====== Growth Metrics ========
# ==============================
import pandas as pd
import logging
from metrics_utils import map_area_to_alias 

def growth_patterns_county(county_summary_initial, county_summary_final, modelrun_id, modelrun_alias, plan, output_path):
    """
    Calculates the growth in total households (TotHH) and total jobs (TotJobs) at the county level, 
    between an initial and a final summary period, and assigns the share of growth in households and jobs to each county.

    Parameters:
    - county_summary_initial (DataFrame): Pandas DataFrame containing county level summary data for the initial period.
    - county_summary_final (DataFrame): Pandas DataFrame containing county level summary data for the final period.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - plan (str): Plan name (e.g., 'pba50', 'pba50plus').
    - output_path (str): File path for saving the output results.

    Returns:
    - DataFrame: A Pandas DataFrame containing the combined initial and final growth totals for households and jobs by county.
    """
    column_mapping = {"pba50": ('TOTEMP', 'TOTHH', 'COUNTY_NAME'),
                      "pba50plus": ('totemp', 'tothh', 'county')}
    
    total_job_column, total_hh_column, county_column = column_mapping.get(plan.lower(), column_mapping['pba50plus']) 

    def calculate_totals(df, year):
        df_totals = df.groupby(county_column, as_index=False).agg(TotHH=(total_hh_column, 'sum'),
                                                                  TotJobs=(total_job_column, 'sum'))
        df_totals['year'] = year
        df_totals['modelrun_id'] = modelrun_id
        df_totals['modelrun_alias'] = f"{year} {modelrun_alias}"
        df_totals['hh_share_of_growth'] = None
        df_totals['jobs_share_of_growth'] = None
        return df_totals
    
    # Calculate totals for initial and final years
    totals_initial = calculate_totals(county_summary_initial, 2015 if plan == "pba50" else 2020)
    totals_final = calculate_totals(county_summary_final, 2050)

    totals_final = totals_final.merge(totals_initial[[county_column, 'TotHH', 'TotJobs']], on=county_column, suffixes=('', '_initial'))

    totals_final['hh_growth'] = totals_final['TotHH'] - totals_final['TotHH_initial']
    totals_final['jobs_growth'] = totals_final['TotJobs'] - totals_final['TotJobs_initial']

    # Calculate the total growth in households and jobs across all counties
    total_hh_growth = totals_final['hh_growth'].sum()
    total_jobs_growth = totals_final['jobs_growth'].sum()

    totals_final['hh_share_of_growth'] = (totals_final['hh_growth'] / total_hh_growth).round(3)
    totals_final['jobs_share_of_growth'] = (totals_final['jobs_growth'] / total_jobs_growth).round(3)

    # Drop initial totals columns as they're no longer needed
    totals_final.drop(['TotHH_initial', 'TotJobs_initial'], axis=1, inplace=True)

    # Concatenate the initial and final totals.
    combined_df = pd.concat([totals_initial.drop(['hh_share_of_growth', 'jobs_share_of_growth'], axis=1), totals_final], ignore_index=True)
    combined_df['metric_type'] = 'growth_county'
    combined_df.rename(columns={county_column: 'county'}, inplace=True)
    # Select and order the columns  
    combined_df = combined_df[['modelrun_id', 'modelrun_alias', 'county', 'TotHH', 'TotJobs', 'hh_share_of_growth', 'jobs_share_of_growth']]

    return combined_df


def growth_patterns_geography(parcel_geog_summary_initial: pd.DataFrame, 
                              parcel_geog_summary_final: pd.DataFrame, 
                              modelrun_id: str, 
                              modelrun_alias: str, 
                              plan: str, 
                              output_path: str) -> pd.DataFrame:
    """
    Calculates the growth in total households (TotHH) and total jobs (TotJobs) at different geographic levels,
    between an initial and a final summary period, and assigns the share of growth in households and jobs to each area.

    Parameters:
    - parcel_geog_summary_initial (DataFrame): Pandas DataFrame containing parcel level summary data for the initial period.
    - parcel_geog_summary_final (DataFrame): Pandas DataFrame containing parcel level summary data for the final period.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - plan (str): Plan name (e.g., 'pba50', 'pba50plus').
    - output_path (str): File path for saving the output results.

    Returns:
    - DataFrame: A Pandas DataFrame containing the combined initial and final growth totals for households and jobs by geography.
    """
     
    def calculate_growth_geog(df, area, year):
        total_hhs = df['tothh'].sum()
        total_emp = df['totemp'].sum()
        return {'modelrun_id': modelrun_id,
                'modelrun_alias': f"{year} {modelrun_alias}",
                'area': area,
                'area_alias': map_area_to_alias(area),
                'TotHH': total_hhs,
                'TotJobs': total_emp}
    
    # Define the years to process
    years_data = {'initial': (parcel_geog_summary_initial, '2015' if plan == "pba50" else '2020'),
                  'final': (parcel_geog_summary_final, '2050')}
    
    # Define area filters
    if plan == "pba50":
        area_filters = {'HRA': lambda df: df['hra_id'] == 1,
                        'TRA': lambda df: df['tra_id'] == 1,
                        'HRAandTRA': lambda df: (df['tra_id'] == 1) & (df['hra_id'] == 1),
                        'GG': lambda df: df['gg_id'] == 1,
                        'EPC': lambda df: df['coc_flag_pba2050'] == 1,
                        'Region': None}
    elif plan == "pba50plus":
        area_filters = {'HRA': lambda df: df['hra_id'] == 'HRA',
                        'TRA': lambda df: df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']),
                        'HRAandTRA': lambda df: (df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])) & (df['hra_id'] == 'HRA'),
                        'GG': lambda df: df['gg_id'] == 'GG',
                        'PDA': lambda df: df['pda_id'] != 'NA', # this should be modified
                        'EPC': lambda df: df['epc_id'] == 'EPC',
                        'Region': None}

    growth_df = []

    # Process each area filter and calculate growth patterns
    for year_key, (df, year) in years_data.items():
        for area, filter_condition in area_filters.items():
            if filter_condition is not None:
                if callable(filter_condition):  # Check if the filter is a function
                    df_area = df[filter_condition(df)]
                else:
                    df_area = df[df[filter_condition] == area]
            else:
                df_area = df
                
            area_result = calculate_growth_geog(df_area, area, year)
            growth_df.append(area_result)

    result_growth_share = pd.DataFrame(growth_df)
    
    result_growth_share = result_growth_share[['modelrun_id', 'modelrun_alias', 'area', 'area_alias', 'TotHH', 'TotJobs']]

    result_growth_share['metric_type'] = 'growth_geography'
    return result_growth_share


    # Calculate the total growth to find shares
    # total_hh_growth = growth_df['hh_growth'].sum()
    # total_jobs_growth = growth_df['jobs_growth'].sum()

    # Calculate shares of growth
    # growth_df['hh_share_of_growth'] = (growth_df['hh_growth'] / total_hh_growth).round(3)
    # growth_df['jobs_share_of_growth'] = (growth_df['jobs_growth'] / total_jobs_growth).round(3)

    # Remove the growth columns if not needed in the final output
    # growth_df.drop(['hh_growth', 'jobs_growth'], axis=1, inplace=True)