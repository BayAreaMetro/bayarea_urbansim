# ==============================
# ====== Growth Metrics ========
# ==============================
import pandas as pd
import logging
import metrics_utils

def growth_patterns_county(rtp, modelrun_alias, modelrun_id, modelrun_data, regional_hh_jobs_dict, output_path, append_output):
    """
    Calculates the growth in total households and total jobs at the county level, 
    between an initial and a final summary period, and assigns the share of growth in households and jobs to each county.

    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - regional_hh_jobs_dict (dict) ->  year -> { 'total_households': int, 'total_jobs': int }
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
    logging.info("Calculating growth_patterns_county")
    RTP_COLUMNS = {
        "RTP2021": ('TOTEMP', 'TOTHH', 'COUNTY_NAME'),
        "RTP2025": ('totemp', 'tothh', 'county')
    }
    total_job_column, total_hh_column, county_column = RTP_COLUMNS[rtp]
    
    # Calculate totals for initial and final years
    SUMMARY_YEARS = sorted(modelrun_data.keys())
    year_initial = SUMMARY_YEARS[0]
    year_horizon = SUMMARY_YEARS[-1]

    summary_dfs = {}
    for year in SUMMARY_YEARS:
        # add parcel total / county total for total_households, total_jobs 
        regional_hh_jobs_dict[year]['hh_county_over_parcel'] = \
            regional_hh_jobs_dict[year]['total_households'] / modelrun_data[year]['county'][total_hh_column].sum()
        regional_hh_jobs_dict[year]['jobs_county_over_parcel'] = \
            regional_hh_jobs_dict[year]['total_jobs'] / modelrun_data[year]['county'][total_job_column].sum()

        summary_dfs[year] = modelrun_data[year]["county"].copy()
        # rename columns to standardized version
        summary_dfs[year].rename(columns={
            total_hh_column :'total_households',
            total_job_column:'total_jobs',
            county_column   :'county'}, inplace=True)
        summary_dfs[year]['modelrun_alias'] = f"{year} {modelrun_alias}"
        logging.debug("summary_dfs[year]:\n{}".format(summary_dfs[year]))

        # scale to regional_hh_jobs_dict so it's consistent with the results from growth_patterns_geography()
        summary_dfs[year]['total_households'] = \
            summary_dfs[year].total_households * regional_hh_jobs_dict[year]['hh_county_over_parcel']
        summary_dfs[year]['total_jobs'] = \
            summary_dfs[year].total_jobs * regional_hh_jobs_dict[year]['jobs_county_over_parcel']
        logging.debug("summary_dfs[year]:\n{}".format(summary_dfs[year]))

    logging.debug(f"regional_hh_jobs_dict: {regional_hh_jobs_dict}")

    # join summary initial to summary final
    summary_final = pd.merge(
        left  = summary_dfs[year_horizon],
        right = summary_dfs[year_initial],
        on    = 'county',
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
    combined_df = pd.concat([summary_dfs[year_initial], summary_final], ignore_index=True)
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

    Writes metrics_growthPattern_geographies.csv to output_path, apeending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - area
    - total_[households|jobs]
    - [hh|jobs]_growth
    - [hh|jobs]_share_of_growth

    Returns { initial_year|horizon_year:{
        'total_households': number of total households from parcel table, 
        'total_jobs'      : number of total jobs from parcel table}
    }
    """
    logging.info("Calculating growth_patterns_geography")

    SUMMARY_YEARS = sorted(modelrun_data.keys())

    # Process each area filter and calculate growth patterns
    summary_dfs = []
    return_dict = {}
    for year in SUMMARY_YEARS:
        summary_list = [] # list of dicts for this year
        for area, filter_condition in metrics_utils.PARCEL_AREA_FILTERS[rtp].items():
            if callable(filter_condition):  # Check if the filter is a function]
                df_area = modelrun_data[year]['parcel'].loc[filter_condition(modelrun_data[year]['parcel'])]
            elif filter_condition == None:
                df_area = modelrun_data[year]['parcel']
            logging.debug("area={} df_area len={:,}".format(area, len(df_area)))

            summary_list.append({
                'modelrun_alias'  : f"{year} {modelrun_alias}",
                'area'            : area,
                'total_households': df_area['tothh'].sum(),
                'total_jobs'      : df_area['totemp'].sum()
            })
            # save regional numbers for return_dict
            if area == 'Region':
                return_dict[year] = {
                    'total_households': df_area['tothh'].sum(),
                    'total_jobs'      : df_area['totemp'].sum()
                }
        summary_dfs.append( pd.DataFrame(summary_list))

    # join summary initial to summary final
    summary_final = pd.merge(
        left  = summary_dfs[1],
        right = summary_dfs[0].drop(columns=['modelrun_alias']),
        on    = 'area',
        suffixes=('', '_initial')
    )
    logging.debug("summary_final:\n{}".format(summary_final))

    summary_final['hh_growth']   = summary_final['total_households'] - summary_final['total_households_initial']
    summary_final['jobs_growth'] = summary_final['total_jobs'      ] - summary_final['total_jobs_initial']

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

    filename = "metrics_growthPattern_geographies.csv"
    filepath = output_path / filename

    combined_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(combined_df), filepath))

    return return_dict


def office_vacancy_bldg(
    rtp: str,
    modelrun_alias: str,
    modelrun_id: str,
    modelrun_data: dict,
    run_directory_path: str,
    box_dir: str,
    m_drive: str,
    metrics_dir: str,
    output_path: str,
    append_output: bool,
):
    """
    Calculates office space vacancy rates at different geographic levels.

    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - run_directory_path (str): Path to the run directory.
    - box_dir (str): Path to the Box directory.
    - m_drive (str): Path to the M drive.
    - metrics_dir (str): Path to the metrics directory.
    - output_path (str): File path for saving the output results
    - append_output (bool): True if appending output; False if writing

    Writes metrics_growthPattern_office_data_buildings{by_misc_areas|area_type|county|superdistrict}.csv to output_path, appending if append_output is True. Columns are:
    
    - year
    - area
    - modelrun_id
    - modelrun_alias
    - jobs_office
    - job_spaces_office
    - job_spaces_office_vacant
    - job_spaces_office_vacant_percent
    - jobs
    - job_spaces
    - job_spaces_vacant
    - job_spaces_vacant_percent

    """
    logging.info("Calculating office space")

    # Guard clause: this metric is implemented for RTP2025 / PBA50+ only
    if rtp != "RTP2025":
        logging.info("  RTP2021 is not supported - skipping")
        return

    import geopandas as gpd

    OFFICE_SQFT_PER_JOB = 355

    bayareafips = {
        "06001": "Alameda",
        "06013": "Contra Costa",
        "06041": "Marin",
        "06055": "Napa",
        "06075": "San Francisco",
        "06081": "San Mateo",
        "06085": "Santa Clara",
        "06097": "Sonoma",
        "06095": "Solano",
    }
    SUMMARY_YEARS = sorted(modelrun_data.keys())

    # get TAZs - they have a mapping to superdistricts
    ZONE_PATH = (
        box_dir
        / "Modeling and Surveys"
        / "Urban Modeling"
        / "Spatial"
        / "Zones"
        / "TAZ1454"
        / "zones1454.shp"
    )
    taz1454 = gpd.read_file(ZONE_PATH)
    logging.info(f"  Reading TAZs from {ZONE_PATH}...")
    taz_x_sd = taz1454.set_index("taz1454").superdistr

    # get parcels to TAZ crosswalk file
    TAZ_CROSSWALK_FILE = (
        m_drive
        / "urban_modeling"
        / "baus"
        / "BAUS Inputs"
        / "basis_inputs"
        / "crosswalks"
        / "2020_08_17_parcel_to_taz1454sub.csv"
    )

    parcel_to_taz_xwalk = pd.read_csv(TAZ_CROSSWALK_FILE, index_col="PARCEL_ID")
    parcel_to_taz_xwalk["county"] = parcel_to_taz_xwalk.manual_county.map(
        lambda x: f"06{x:03d}"
    ).map(bayareafips)

    logging.info(f"  Reading parcel_to_taz1454sub from {TAZ_CROSSWALK_FILE}...")

    # convenience series for crosswalking
    parcel_x_zone = parcel_to_taz_xwalk.ZONE_ID
    parcel_x_sd = parcel_x_zone.map(taz_x_sd)
    parcel_x_county = parcel_to_taz_xwalk.county

    # Define convenience function for finalizing the df before outputting, to be run on different summary levels
    def finalize_output(df):

        df["job_spaces_office_vacant_percent"] = (
            df["job_spaces_office_vacant"] / df["job_spaces_office"]
        )
        df["job_spaces_vacant_percent"] = (
            df["job_spaces_vacant"] / df["job_spaces"]
        )
        df = df[
            [
                "modelrun_id",
                "modelrun_alias",
                "area",
                "year",
                "jobs",
                "job_spaces",
                "job_spaces_vacant",
                "job_spaces_vacant_percent",
                "jobs_office",
                "job_spaces_office",
                "job_spaces_office_vacant",
                "job_spaces_office_vacant_percent",
            ]
        ]
        return df

    def building_loader(year):
        # Read in buildings
        modelrun_name = modelrun_id
        # Sometimes the modelrun_id is a whole file path
        # Handle both forms of slashes in this field
        if "\\" in modelrun_id:
            modelrun_name = modelrun_id.split("\\")[-1]
        if "/" in modelrun_id:
            modelrun_name = modelrun_id.split("/")[-1]

        BUILDINGS_PATH = (
            run_directory_path
            / f"core_summaries/{modelrun_name}_building_summary_{year}.csv"
        )
        logging.info(f"  Reading buildings_output_summary from {BUILDINGS_PATH}...")

        buildings_output = pd.read_csv(
            BUILDINGS_PATH,
            usecols=[
                "parcel_id",
                #"year_built",
                "non_residential_sqft",
                "building_type",
                "job_spaces",
            ],
            dtype={"parcel_id": int},
        )
        return buildings_output

    # store data from loops over year and geography
    summary_dict = {}

    # this loop goes over year and gets buildings and parcel data, including interpolating buildings to 2023 if that is the received year

    for year in SUMMARY_YEARS:

        logging.info(f"  Processing {year}...")

        if year==2023:

            # interpolation per DL's approach
            y1, y2 = 2020, 2025
            b1 = building_loader(y1)
            try:
                b2 = building_loader(y2)
            except FileNotFoundError:
                logging.debug("  Building summary file not found for year %d" % y2)

                b2 = b1 
            logging.info(f"  Interpolating buildings {y1} and {y2} to {year}...")
            df = b1.copy()

            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]) and col!='parcel_id':

                    # Long way to write 3/5 but maybe it'll pay off in future... :)
                    df[col] = b1[col] + ((2023 - y1) / (y2 - y1))*(b2[col] - b1[col])

            # return the interpolated version
            buildings_output = df

        else:
            # get either 2020 single year, or 2050 single year
            buildings_output = building_loader(year)

        # get parcels
        parcel_output = modelrun_data[year]["parcel"]
        parcel_output["zone_id"] = parcel_output["parcel_id"].map(parcel_x_zone)
        parcel_output["superdistrict"] = parcel_output["parcel_id"].map(parcel_x_sd)
        parcel_output["county"] = parcel_output["parcel_id"].map(parcel_x_county)

        # quick if redundant recoding
        parcel_output['jobs'] = parcel_output.totemp
        
        # assign job spaces - all types - from buildings back to parcels
        parcel_output["job_spaces"] = parcel_output.parcel_id.map(
            buildings_output.groupby("parcel_id")["job_spaces"].sum()
        )
        parcel_output["job_spaces"].fillna(0, inplace=True)

        # subset buildings to office / mixed office
        buildings_output = buildings_output[
            buildings_output["building_type"].isin(["OF", "ME"])
        ]
        logging.info(f"  {len(buildings_output)} office buildings")

        buildings_output["zone_id"] = buildings_output["parcel_id"].map(parcel_x_zone)
        buildings_output["superdistrict"] = buildings_output["parcel_id"].map(
            parcel_x_sd
        )

        # add job spaces - now from the buildings subsetted to office only
        parcel_output["job_spaces_office"] = parcel_output.parcel_id.map(
            buildings_output.groupby("parcel_id")["job_spaces"].sum()
        )
        parcel_output["job_spaces_office"].fillna(0, inplace=True)

        # classify parcel by presence of office building

        parcel_has_office_mask = parcel_output.job_spaces_office > 0

        # we assume that a job on a parcel with an office building is an office job

        parcel_output["jobs_office"] = parcel_output.totemp * parcel_has_office_mask
        parcel_output["jobs_office"].fillna(0, inplace=True)

        # calculate vacant job spaces - avoid negatives with clipping
        parcel_output["job_spaces_office_vacant"] = (
            parcel_output["job_spaces_office"] - parcel_output["jobs_office"]
        ).clip(0)

        # also get all-type job space vacant counts - avoid negatives with clipping
        parcel_output["job_spaces_vacant"] = (
            parcel_output["job_spaces"] - parcel_output["jobs"]
        ).clip(0)

        
        # don't include percentages for the summaries
        val_cols_summary = ['jobs_office','job_spaces_office','job_spaces_office_vacant',
                    'jobs','job_spaces','job_spaces_vacant']

        logging.info(
            f'  Parcel office summary: {parcel_output[["job_spaces_office","jobs_office","job_spaces_office_vacant"]].sum()}'
        )

        # Process each area filter and get office inventory / vacancy
        # This leverages the filters already set up elsewhere - but we could just
        # pass a list of a few area columns and group by those, and collect the results

        for area, filter_condition in metrics_utils.PARCEL_AREA_FILTERS[rtp].items():
            logging.info(f"  Processing {area}...")
            if callable(filter_condition):  # Check if the filter is a function]
                df_area = parcel_output.loc[filter_condition(parcel_output)]
            elif filter_condition == None:
                df_area = parcel_output
            logging.debug("area={} df_area len={:,}".format(area, len(df_area)))

            # this_area_summary = pd.Series(
            #     {
            #         "modelrun_id": modelrun_id,
            #         "modelrun_alias": f"{year} {modelrun_alias}",
            #         #'area'                      : area,
            #         "jobs_office": df_area["jobs_office"].sum(),
            #         "job_spaces_office": df_area["job_spaces_office"].sum(),
            #         "job_spaces_office_vacant": df_area[
            #             "job_spaces_office_vacant"
            #         ].sum(),
            #     }
            # )
            
            this_area_summary = pd.concat([df_area[val_cols_summary].sum()],
                                          keys=[(modelrun_alias,modelrun_id)],
                                          names=["modelrun_alias","modelrun_id"])

            summary_dict[(year, area)] = this_area_summary

        # combine yearly and area summaries in one df
        area_summary_final = (
            pd.concat(summary_dict, names=["year", "area"])
            .unstack(4)
            .reset_index()
        )


        # do the last prep and formating
        area_summary_final = finalize_output(area_summary_final)

        logging.debug("area_summary_final:\n{}".format(area_summary_final))

        filename = "metrics_growthPattern_office_data_buildings_by_misc_areas.csv"
        filepath = output_path / filename

        # write out the area_summary_final df
        area_summary_final.to_csv(
            filepath,
            mode="a" if append_output else "w",
            header=False if append_output else True,
            index=False,
        )
        logging.info(
            "{} {:,} lines to {}".format(
                "Appended" if append_output else "Wrote", len(area_summary_final), filepath
            )
        )

        # lastly, prepare county and sd level summaries as well
        for geo in ["superdistrict", "county", "area_type"]:
            geo_summary = pd.concat(
                [
                    parcel_output.groupby(geo)[
                        ["jobs_office", "job_spaces_office", "job_spaces_office_vacant"]+\
                            ["jobs", "job_spaces", "job_spaces_vacant"]
                    ].sum()
                ],
                keys=[(modelrun_id, f"{year} {modelrun_alias}")],
                names=["modelrun_id", "modelrun_alias", "area"],
            )
            geo_summary = geo_summary.reset_index()
            geo_summary["year"] = year

            # get vacancy and organize cols
            geo_summary = finalize_output(geo_summary)
            filename = f"metrics_growthPattern_office_data_buildings_{geo}.csv"
            filepath = output_path / filename
            logging.debug("geo_summary_final-{}:\n{}".format(geo, geo_summary))

            geo_summary.to_csv(
                filepath,
                mode="a" if append_output else "w",
                header=False if append_output else True,
                index=False,
            )
            logging.info(
                "{} {:,} lines to {}".format(
                    "Appended" if append_output else "Wrote", len(geo_summary), filepath
                )
            )


def office_space_summary(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        output_path: str,
        append_output: bool
    ):
    """
    Calculates office space vacancy rates at different geographic levels from zone level data.
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing
    
    Writes metrics_growthPattern_office_space_data_{county|superdistrict}.csv to output_path, appending if append_output is True. Columns are:
    - year
    - area
    - modelrun_id
    - modelrun_alias
    - county|superdistrict
    - jobs_office
	- job_spaces_office
	- job_spaces_office_vacant
	- job_spaces_office_vacant_percent
	- jobs
	- job_spaces
	- job_spaces_vacant
	- job_spaces_vacant_percent

    """

    # non-residential_vacancy : vacant job spaces as a share of total job spaces
    # office vacancy: vacant office job spaces as a share of total office job spaces
    # These are calculated as follows in the BAUS summary code:

    # zones['non_residential_vacancy_office'] = (buildings.query('building_type=="OF"')
    #                                         .groupby(['zone_id'])
    #                                         .apply(lambda x: x['vacant_job_spaces'].sum().clip(0) /
    #                                         x['job_spaces'].sum().clip(1))
    #                                         )

    # these are zone level data and for aggregation purposes we don't want percentages but instead the components of the fraction
    # we have job_spaces_office, but not vacant_job_spaces_office. Given the above, we back that out by
    # job_spaces_office_vacant = non_residential_vacancy_office * job_spaces_office


    # Guard clause: this metric is implemented for RTP2025 / PBA50+ only
    if rtp != 'RTP2025':
        logging.info("  RTP2021 is not supported - skipping")
        return

    logging.info("Calculating office space summaries")

    required_cols = ['job_spaces_office','job_spaces','non_residential_vacancy_office','non_residential_vacancy']

    id_cols = [
                "TAZ1454",
                "COUNTY_NAME",
                "SD"]


    # Calculate ratios for initial and final years
    SUMMARY_YEARS = sorted(modelrun_data.keys())

    SUMMARY_GEOGRAPHIES = ["county", "superdistrict"]

    for summary_geography in SUMMARY_GEOGRAPHIES:
        output_dict = {}

        
        for year in SUMMARY_YEARS:
            logging.info(f"  Summarizing for {year=} {summary_geography=}")

            # superdistrict summaries will be from TAZ1454 table
            data_df = modelrun_data[year]["TAZ1454"].copy()

            if not all(x in data_df.columns for x in required_cols):
                logging.info(f" Required columns {required_cols} not found in interim zoning data. Aborting for this run.")
                return
            # calculate the vacancy columns for office space
            data_df['job_spaces_office_vacant'] = (data_df['non_residential_vacancy_office'] * data_df['job_spaces_office']).round(0)
            data_df['job_spaces_office_vacant_percent'] = (data_df['job_spaces_office_vacant'] / data_df['job_spaces_office'])
            data_df['jobs_office'] = data_df['job_spaces_office'] - data_df['job_spaces_office_vacant']
            
            # calculate the vacancy columns for non-residential space in general
            data_df['job_spaces_vacant'] = (data_df['non_residential_vacancy'] * data_df['job_spaces']).round(0)
            data_df['job_spaces_vacant_percent'] = (data_df['job_spaces_vacant'] / data_df['job_spaces'])
            data_df['jobs'] = data_df['job_spaces'] - data_df['job_spaces_vacant']

            # remove the vacancy columns (in percentage terms)
            #required_cols = [c for c in required_cols if 'vacancy' not in c]
            #required_cols += ['job_spaces_office_vacant', 'job_spaces_vacant','jobs_office']
            
            data_df.rename(columns={'COUNTY_NAME':'county','COUNTY':'county','SD':'superdistrict'}, inplace=True)

            # Select and order the columns
            val_cols = ['jobs_office','job_spaces_office','job_spaces_office_vacant','job_spaces_office_vacant_percent',
                    'jobs','job_spaces','job_spaces_vacant','job_spaces_vacant_percent',]
        
            # summarize the data by geo
            data_summary = data_df.groupby(summary_geography)[val_cols].sum()
            # add a regional summary
            data_summary_reg = pd.concat([data_summary.sum()],keys=['Region'],names=[summary_geography]).unstack(1)

            # combine the two
            this_data_combo = pd.concat([data_summary, data_summary_reg],axis=0)
            
            output_dict[(year,summary_geography)] = this_data_combo
            
    
        # collect the data into a single dataframe
        all_data_combo = pd.concat(output_dict, names=['year','area']).reset_index()

        # after summarizing by geography, we need to re-calculate percentages
        all_data_combo['job_spaces_office_vacant_percent'] =all_data_combo['job_spaces_office_vacant'] /all_data_combo['job_spaces_office'] 
        all_data_combo['job_spaces_vacant_percent'] =all_data_combo['job_spaces_vacant'] /all_data_combo['job_spaces'] 

        # add the year and modelrun_id
        
        all_data_combo['year']               = year
        all_data_combo['modelrun_id']        = modelrun_id
        all_data_combo['modelrun_alias']     = f"{year} {modelrun_alias}"
        logging.debug("interim zone all_data_combo:\n{}".format(all_data_combo))
        
        
        all_data_combo = all_data_combo[['year','area','modelrun_id', 'modelrun_alias', summary_geography]+ val_cols]

        # set filename and write it
        filename = f'metrics_growthPattern_office_data_interim_{summary_geography}.csv'
        filepath = output_path / filename

        all_data_combo.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
        logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(all_data_combo), filepath))
