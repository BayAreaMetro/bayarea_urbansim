# ==============================
# ====== Growth Metrics ========
# ==============================
import pandas as pd
import logging
from metrics_utils import map_area_to_alias 

def growth_patterns_county(rtp, modelrun_alias, modelrun_id, modelrun_data, output_path, append_output):
    """
    Calculates the growth in total households and total jobs at the county level, 
    between an initial and a final summary period, and assigns the share of growth in households and jobs to each county.

    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str): File path for saving the output results
    - append_output (bool): True if appending output; False if writing

    Writes metrics_growthPattern_county.csv to output_path, apeending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - county
    - total_[households|jobs]
    - [hh|jobs]_growth
    - [hh|jobs]_share_of_growth
    """
    logging.info("Calculating_growth_patterns_county")
    RTP_COLUMNS = {
        "RTP2021": ('TOTEMP', 'TOTHH', 'COUNTY_NAME'),
        "RTP2025": ('totemp', 'tothh', 'county')
    }
    total_job_column, total_hh_column, county_column = RTP_COLUMNS[rtp]
    
    # Calculate totals for initial and final years
    SUMMARY_YEARS = sorted(modelrun_data.keys())

    summary_initial = modelrun_data[SUMMARY_YEARS[0]]["county"].groupby(county_column, as_index=False).agg(
            total_households=(total_hh_column, 'sum'),
            total_jobs=(total_job_column, 'sum'))
    summary_initial['modelrun_alias'] = f"{SUMMARY_YEARS[0]} {modelrun_alias}"

    summary_final   = modelrun_data[SUMMARY_YEARS[-1]]["county"].groupby(county_column, as_index=False).agg(
            total_households=(total_hh_column, 'sum'),
            total_jobs=(total_job_column, 'sum'))
    summary_final['modelrun_alias'] = f"{SUMMARY_YEARS[-1]} {modelrun_alias}"

    # join summary initial to summary final
    summary_final = pd.merge(
        left  = summary_final,
        right = summary_initial,
        on    = county_column,
        suffixes=('', '_initial')
    )

    summary_final['hh_growth']   = summary_final['total_households'] - summary_final['total_households_initial']
    summary_final['jobs_growth'] = summary_final['total_jobs']       - summary_final['total_jobs_initial']
    logging.debug("summary_final:\n{}".format(summary_final))

    # Calculate the total growth in households and jobs across all counties
    total_hh_growth   = summary_final['hh_growth'].sum()
    total_jobs_growth = summary_final['jobs_growth'].sum()

    summary_final['hh_share_of_growth']   = summary_final['hh_growth'] / total_hh_growth
    summary_final['jobs_share_of_growth'] = summary_final['jobs_growth'] / total_jobs_growth

    # Drop initial totals columns as they're no longer needed
    summary_final.drop(columns=['total_households_initial', 'total_jobs_initial'], inplace=True)
    logging.debug("summary_final:\n{}".format(summary_final))

    # Concatenate the initial and final totals.
    combined_df = pd.concat([summary_initial, summary_final], ignore_index=True)
    combined_df.rename(columns={county_column: 'county'}, inplace=True)
    combined_df['modelrun_id'] = modelrun_id
    # Select and order the columns  
    combined_df = combined_df[[
        'modelrun_id', 'modelrun_alias', 'county', 
        'total_households', 'total_jobs', 
        'hh_growth', 'jobs_growth',
        'hh_share_of_growth', 'jobs_share_of_growth']]
    logging.debug("combined_df:\n{}".format(combined_df))

    filename = "metrics_growthPattern_county.csv"
    filepath = output_path / filename

    combined_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(combined_df), filepath))

def growth_patterns_geography(rtp: str,
                              modelrun_alias: str, 
                              modelrun_id: str, 
                              modelrun_data: dict, 
                              output_path: str,
                              append_output: bool):
    """
    Calculates the growth in total households (total_households) and total jobs (total_jobs) at different geographic levels,
    between an initial and a final summary period, and assigns the share of growth in households and jobs to each area.

    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str): File path for saving the output results
    - append_output (bool): True if appending output; False if writing

    Writes metrics_growthPattern_county.csv to output_path, apeending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - area
    - total_[households|jobs]
    - [hh|jobs]_growth
    - [hh|jobs]_share_of_growth

    """
    logging.info("Calculating growth_patterns_geography")
    
    # Define area filters
    if rtp == "RTP2021":
        area_filters = {
            'HRA'      : lambda df: df['hra_id'] == 'HRA',
            'TRA'      : lambda df: df['tra_id'] != 'NA',  # note this is the string NA
            'HRAandTRA': lambda df: (df['tra_id'] != 'NA') & (df['hra_id'] == 'HRA'),
            'GG'       : lambda df: df['gg_id'] == 'GG',
            'PDA'      : lambda df: pd.notna(df['pda_id_pba50_fb']),
            'EPC'      : lambda df: df['coc_flag_pba2050'] == 1,
            'Region'   : None
    }
    if rtp == "RTP2025":
        area_filters = {
            'HRA'      : lambda df: df['hra_id'] == 'HRA',
            'TRA'      : lambda df: df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']),
            'HRAandTRA': lambda df: (df['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])) & (df['hra_id'] == 'HRA'),
            'GG'       : lambda df: df['gg_id'] == 'GG',
            'PDA'      : lambda df: pd.notna(df['pda_id']), # this should be modified
            'EPC'      : lambda df: df['epc_id'] == 'EPC',
            'Region'   : None
        }


    SUMMARY_YEARS = sorted(modelrun_data.keys())

    # Process each area filter and calculate growth patterns
    summary_dfs = []
    for year in SUMMARY_YEARS:
        summary_list = [] # list of dicts
        for area, filter_condition in area_filters.items():
            if callable(filter_condition):  # Check if the filter is a function]
                df_area = modelrun_data[year]['parcel'][filter_condition(modelrun_data[year]['parcel'])]
            else:
                df_area = modelrun_data[year]['parcel']
            logging.debug("area={} df_area len={:,}".format(area, len(df_area)))
                
            summary_list.append({
                'modelrun_alias'  : f"{year} {modelrun_alias}",
                'area'            : area,
                'total_households': df_area['tothh'].sum(),
                'total_jobs'      : df_area['totemp'].sum()
            })
        summary_dfs.append( pd.DataFrame(summary_list))
    
    # join summary initial to summary final
    summary_final = pd.merge(
        left  = summary_dfs[1],
        right = summary_dfs[0].drop(columns=['modelrun_alias']),
        on    = 'area',
        suffixes=('', '_initial')
    )
    logging.debug("summary_final:\n{}".format(summary_final))

    summary_final['hh_growth']   = summary_final['total_households'  ] - summary_final['total_households_initial']
    summary_final['jobs_growth'] = summary_final['total_jobs'] - summary_final['total_jobs_initial']

    # Calculate the total regional growth to find shares
    total_hh_growth   = summary_final.loc[summary_final.area=='Region', 'hh_growth'].sum()
    total_jobs_growth = summary_final.loc[summary_final.area=='Region', 'jobs_growth'].sum()

    # Calculate shares of growth
    summary_final['hh_share_of_growth']   = summary_final['hh_growth'] / total_hh_growth
    summary_final['jobs_share_of_growth'] = summary_final['jobs_growth'] / total_jobs_growth

    # Remove the growth columns if not needed in the final output
    summary_final.drop(columns=['total_households_initial', 'total_jobs_initial'], inplace=True)
    logging.debug("summary_final:\n{}".format(summary_final))

    # Concatenate the initial and final totals.
    combined_df = pd.concat([summary_dfs[0], summary_final], ignore_index=True)
    combined_df['modelrun_id'] = modelrun_id
    combined_df = combined_df[['modelrun_id','modelrun_alias','area',
                               'total_households','total_jobs',
                               'hh_growth','jobs_growth',
                               'hh_share_of_growth','jobs_share_of_growth']]

    filename = "metrics_growthPattern_geography.csv"
    filepath = output_path / filename

    combined_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(combined_df), filepath))
