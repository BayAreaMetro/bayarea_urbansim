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
    Calculate and export the jobs-to-housing ratio for the initial and final years for each county/superdistrict and the whole region.
    Currently, superdistrict is only implemented for RTP2025.
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing
    
    Writes metrics_vibrant1_jobs_housing_ratio_{county|superdistrict}.csv to output_path, appending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - county|superdistrict
    - total_jobs
    - total_households
    - jobs_housing_ratio

    """
    logging.info("Calculating jobs_housing_ratio")

    # Calculate ratios for initial and final years
    SUMMARY_YEARS = sorted(modelrun_data.keys())

    # county summary only for RTP2021
    SUMMARY_GEOGRAPHIES = ["county"]
    # add in superdistrict for RTP2025
    if rtp == "RTP2025":
        SUMMARY_GEOGRAPHIES = ["county", "superdistrict"]

    for summary_geography in SUMMARY_GEOGRAPHIES:
        all_ratios_df = pd.DataFrame()
        for year in SUMMARY_YEARS:
            logging.info(f"  Summarizing for {year=} {summary_geography=}")

            # copying is fine - these are little
            if summary_geography == "county":
                data_df = modelrun_data[year]["county"].copy()
            else:
                # superdistrict summaries will be from TAZ1454 table
                data_df = modelrun_data[year]["TAZ1454"].copy()

            # standardize data columns to total_households, total_jobs
            if 'totemp' in data_df.columns:
                data_df.rename(columns={'totemp':'total_jobs', 'tothh':'total_households'}, inplace=True)
            if 'TOTEMP' in data_df.columns:
                data_df.rename(columns={'TOTEMP':'total_jobs', 'TOTHH':'total_households'}, inplace=True)
            if 'SD' in data_df.columns:
                data_df.rename(columns={'SD':'superdistrict'}, inplace=True)

            # for supderdistrict, we need to group_by
            if summary_geography == "superdistrict":
                summary_df = data_df.groupby('superdistrict', as_index=False).agg({'total_households':'sum', 'total_jobs':'sum'})
            else:
                summary_df = data_df

            # add regional total
            summary_df = pd.concat([summary_df, pd.DataFrame([{
                    summary_geography      :'Region', 
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

        # Select and order the columns  
        all_ratios_df = all_ratios_df[['modelrun_id', 'modelrun_alias', summary_geography, 
                                       'total_jobs','total_households','jobs_housing_ratio']]

        # write it
        filename = f'metrics_vibrant1_jobs_housing_ratio_{summary_geography}.csv'
        filepath = output_path / filename

        all_ratios_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
        logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(all_ratios_df), filepath))


def gdp_growth(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        box_path: str,
        output_path: str,
        append_output: bool
    ):
    """
    Function calculates GDP from REMI data. There is no variation across scenarios for his metric. 
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame}
    - metrics_path (str or Path): The directory path to the metrics_input_files directory.
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing
    """

    # TODO: consider if we should just have a dict for this - there is no variation between project runs - we could run once and lookup later.
    logging.info("Calculating GDP per capita growth from REMI data")
    if rtp == "RTP2021":
        logging.info("  RTP2021 not supported. Skipping")
        return

    rtp2021_remi_path = box_path / 'Modeling and Surveys' / 'Regional Modeling' / \
        'Regional Forecast PBA50' / 'REMI_raw_output' / \
        'economy' / 'R6H2C_COVID_cross_rev2' / 'Summary.xlsx'

    rtp2025_remi_path = box_path / 'Modeling and Surveys' / 'Regional Modeling' / 'Regional Forecast PBA50 Plus Update' / \
        'REMI_raw_output' / 'economy' / 'REMI31_NC1_RC1_FBP' / 'B5 summary.xlsx'

    def remi_loader(**kwargs):
        
        first_year = (kwargs['process_args']['firstyear'])
        last_year = (kwargs['process_args']['lastyear'])
        years = range(first_year, last_year, 5)

        remi_econ_raw = pd.read_excel(**kwargs['xl_args'])

        df = remi_econ_raw.set_index(['Category']).filter(regex='\d{4}')  # [years]

        # fixed, in 2009 dollars - that's fine since we are reporting on ratios
        # as long as income is reported in fixed dollars

        # get national price index

        us_price_index = df.loc['PCE-Price Index']

        # get scalar for getting from fixed 2009 dollars to 2020 dollars
        us_price_deflator_2020_vs_base_2009 = (
            us_price_index / us_price_index.loc[2009]).loc[2020]

        gdp_per_capita = df.loc['Gross Domestic Product'] / df.loc['Population']

        # get start, end year values
        gdp_per_capita_2020dol = gdp_per_capita.mul(
            1e6).mul(us_price_deflator_2020_vs_base_2009)

        return gdp_per_capita_2020dol.loc[[first_year,last_year]]

    # set up a dict with key processing args for the two RTP variants
    remi_load_params = {'RTP2021': {'xl_args': {'io': rtp2021_remi_path, 'skiprows': 5}, 
                                    'process_args': {'firstyear': 2015, 'lastyear': 2050}},
                        'RTP2025': {'xl_args': {'io': rtp2025_remi_path, 'skiprows': 4}, 
                                    'process_args': {'firstyear': 2020, 'lastyear': 2050}}
                    }

    # returns a series
    gdp_levels = remi_loader(**remi_load_params[rtp])

    fstyr = remi_load_params[rtp]["process_args"]["firstyear"]
    lstyr = remi_load_params[rtp]["process_args"]["lastyear"]
    
    gdp_firstyear = gdp_levels.loc[fstyr]
    gdp_lastyear = gdp_levels.loc[lstyr]

    gdp_growth = gdp_lastyear / gdp_firstyear

    # Add metadata, format, and finally export to CSV

    gdp_growth_df = pd.DataFrame({
        'modelrun_id': modelrun_id,
        'modelrun_alias': modelrun_alias,
        'grp_per_capita_growth': gdp_growth,
        f'grp_per_capita_{fstyr}': gdp_firstyear,
        f'grp_per_capita_{lstyr}': gdp_lastyear,
        'name': 'Regionwide'
    }, index=[0])

    out_file = output_path / 'metrics_vibrant2_grp_percapita_growth.csv'

    gdp_growth_df.to_csv(
        out_file,
        mode='a' if append_output else 'w',
        header=False if append_output else True,
        index=False,
    )

    logging.info(f"{'Appended' if append_output else 'Wrote'} {len(gdp_growth_df)} " \
                 + f"line{'s' if len(gdp_growth_df) > 1 else ''} to {out_file}")


def ppa_job_growth(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        metrics_path: str,
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
    - metrics_path (str or Path): The directory path to the metrics_input_files directory.
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing
    """
    logging.info("Getting jobs by wage level")
    if rtp == "RTP2021":
        logging.info("  RTP2021 not supported. Skipping")
        return

    jobs_file = metrics_path / "metrics_input_files" / "emp_by_ind11_pba2050plus.csv"
    wage_file = metrics_path / "metrics_input_files" / "jobs_wagelevel.csv"
    
    jobsdata = pd.read_csv(jobs_file, index_col=0)
    wagedata = pd.read_csv(wage_file, index_col=0).wage_level

    # Add wage level column to jobs data
    jobsdata['job_cat'] = wagedata
    # Slight string edit for presentation purposes  
    jobsdata['job_cat'] = jobsdata['job_cat'].add('-Wage Industries')

    jobs_by_wagelevel = jobsdata.groupby('job_cat').sum()
    

    jobs_by_wagelevel_growth_2020_2050 = jobs_by_wagelevel['2050'] / jobs_by_wagelevel['2020']
    jobs_by_wagelevel_growth_2020_2050.name = 'Jobs Growth'

    jobsdata['job_cat'] = wagedata
    jobsdata['job_cat'] = jobsdata['job_cat'].add('-Wage Industries')

    jobs_by_wagelevel = jobsdata.groupby('job_cat').sum()

    jobs_by_wagelevel = pd.concat([jobs_by_wagelevel,
            pd.concat([jobs_by_wagelevel.sum(numeric_only=True)], keys=['All Jobs']).unstack(1)], axis=0)
    
    jobs_by_wagelevel.index = jobs_by_wagelevel.index.set_names('job_cat')

    jobs_by_wagelevel_growth_2020_2050 = (jobs_by_wagelevel['2050'] / jobs_by_wagelevel['2020'])-1
    jobs_by_wagelevel_growth_2020_2050.name = 'jobgrowth'
    jobs_by_wagelevel_growth_2020_2050 = jobs_by_wagelevel_growth_2020_2050.reset_index()

    jobs_by_wagelevel_growth_2020_2050[[
        'modelrun_id', 'modelrun_alias']] = modelrun_id, modelrun_alias
    
    # save for later - we will concat with ppa jobs in a bit

    
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
    job_growth_ppa = pd.DataFrame({
        'modelrun_id': modelrun_id,
        'modelrun_alias': modelrun_alias,
        'job_cat': 'PPA',
        'jobgrowth': job_growth_pct
    }, index=[0])

    # Concatenate with jobs by wage level growth
    job_growth_df = pd.concat([job_growth_ppa, jobs_by_wagelevel_growth_2020_2050])

    out_file = output_path / 'metrics_vibrant2_ppa_job_growth.csv'
    job_growth_df.to_csv(
        out_file,
        mode='a' if append_output else 'w',
        header=False if append_output else True,
        index=False,
    )

    logging.info(f"{'Appended' if append_output else 'Wrote'} {len(job_growth_df)} " \
                 + f"line{'s' if len(job_growth_df) > 1 else ''} to {out_file}")
