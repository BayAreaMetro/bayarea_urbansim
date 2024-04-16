# ==============================
# ====== Vibrant Metrics =======
# ==============================

import logging
import pandas as pd
import metrics_utils

def jobs_housing_ratio(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        output_path: str,
        append_output: bool
    ):
    """
    Calculate and export the jobs-to-housing ratio for the initial and final years for each county and the whole region.
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing
    

    """
    logging.info("Calculating jobs_housing_ratio")
    RTP_COLUMNS = {
        "RTP2021": ('TOTEMP', 'TOTHH', 'COUNTY_NAME'),
        "RTP2025": ('totemp', 'tothh', 'county')
    } 
        
    total_job_column, total_hh_column, county_column = RTP_COLUMNS[rtp]

    # Calculate ratios for initial and final years
    SUMMARY_YEARS = sorted(modelrun_data.keys())

    all_ratios_df = pd.DataFrame()
    for year in SUMMARY_YEARS:
        summary_df = modelrun_data[year]["county"].groupby(county_column, as_index=False).agg(
            total_households=(total_hh_column,  'sum'),
            total_jobs      =(total_job_column, 'sum')
        )
        # add regional total
        summary_df = pd.concat([summary_df, pd.DataFrame([{
            county_column      :'Region', 
            'total_households' :summary_df.total_households.sum(),
            'total_jobs'       :summary_df.total_jobs.sum(),
            }])
        ])

        summary_df['jobs_housing_ratio'] = summary_df.total_jobs / summary_df.total_households
        summary_df['year']               = year
        summary_df['modelrun_id']        = modelrun_id
        summary_df['modelrun_alias']     = f"{year} {modelrun_alias}"
        logging.debug("summary_df:\n{}".format(summary_df))
        all_ratios_df = pd.concat([all_ratios_df, summary_df])
    
    # Rename the county column to 'county' regardless of the original name
    all_ratios_df.rename(columns={county_column:'county'}, inplace=True)

    # Select and order the columns  
    all_ratios_df = all_ratios_df[['modelrun_id', 'modelrun_alias', 'county', 
                                   'total_jobs','total_households','jobs_housing_ratio']]

    # write it
    filename = 'metrics_vibrant1_jobs_housing_ratio.csv'
    filepath = output_path / filename

    all_ratios_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(all_ratios_df), filepath))


def ppa_job_growth(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        output_path: str,
        append_output: bool
    ):
    """
    Calculate and export the % growth in jobs in PPAs from the initial to the final year.
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame}
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing
    """
    logging.info("Calculating ppa_job_growth")

    # Identify initial and final year based on keys in modelrun_data
    initial_year = sorted(modelrun_data.keys())[0]
    final_year = sorted(modelrun_data.keys())[-1]

    # Filter to PPA parcels only
    filter_condition = metrics_utils.PARCEL_AREA_FILTERS[rtp]['PPA']
    dfs_area = {}
    for year in modelrun_data.keys():
        dfs_area[year] = modelrun_data[year]['parcel'].loc[filter_condition(modelrun_data[year]['parcel'])]
        logging.info(f"{len(dfs_area[year]):,} parcels in PPAs in year {year}")
    
    # Calculate % growth in jobs
    initial_year_jobs = dfs_area[initial_year]['totemp'].sum()
    final_year_jobs = dfs_area[final_year]['totemp'].sum()
    job_growth_pct = (final_year_jobs - initial_year_jobs) / initial_year_jobs

    # Add metadata, format, and export to CSV
    job_growth_df = pd.DataFrame({
        'modelrun_id': modelrun_id,
        'modelrun_alias': modelrun_alias,
        'job_cat': 'PPA',
        'jobgrowth': job_growth_pct
    }, index=[0])
    out_file = output_path / 'metrics_vibrant2_ppa_job_growth.csv'
    job_growth_df.to_csv(
        out_file,
        mode='a' if append_output else 'w',
        header=False if append_output else True,
        index=False,
    )
    logging.info(f"{'Appended' if append_output else 'Wrote'} {len(job_growth_df)} " \
                 + f"line{'s' if len(job_growth_df) > 1 else ''} to {out_file}")
