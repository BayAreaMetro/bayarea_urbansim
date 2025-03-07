# ==============================
# ====== Healthy Metrics =======
# ==============================

import logging, pathlib, shutil
import pandas as pd
import openpyxl
import xlwings
import metrics_utils

def urban_park_acres(
        BOX_DIR: pathlib.Path,
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        output_path: str,
        append_output: str
    ):
    """
    This method works differently than the others since it works with an Excel workbook.
    1) If append_output == False, it copies the template workbook, 
       PBA50+_DraftBlueprint_UrbanParksMetric.xlsx
       into the output_path as
       metrics_healthy1_UrbanParks.xlsx
    2) It fills in the relevant values (persons by county) into the workbook.

    The metrics_Healthy.twb directly reads from the local modified copy.

    Args:
        rtp (str): RTP2021 or RTP2025.
        modelrun_alias (str): Alias for the model run, used for labeling output.
        modelrun_id (str): Identifier for the model run.
        modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
        output_path (str or Path): The directory path to save the output XLSX file.
        append_output (bool): True if appending output; False if writing

    """
    logging.info("Calculating urban_park_acres")
    if rtp == 'RTP2021':
        logging.info("  RTP2021 is not supported - skipping")
        return
    
    DEST_WORKBOOK   = output_path / "metrics_healthy1_UrbanParks.xlsx"
    # first run -- copy template
    if append_output == False:
        SOURCE_WORKBOOK = BOX_DIR / "Plan Bay Area 2050+/Performance and Equity/Plan Performance/Equity_Performance_Metrics/Healthy_Metrics_Templates/PBA50+_DraftBlueprint_UrbanParksMetric.xlsx"
        shutil.copy(SOURCE_WORKBOOK, DEST_WORKBOOK)
        logging.info(f" Copied source workbook {SOURCE_WORKBOOK}")
        logging.info(f" to destination workbook {DEST_WORKBOOK}")
    
    workbook = openpyxl.load_workbook(filename=DEST_WORKBOOK)
    logging.debug("  Workbook sheetnames: {}".format(workbook.sheetnames))
    sheet = workbook["Dashboard"]
    logging.debug(f"  dimensions:{sheet.dimensions}")

    # summarize by year
    for year_idx, year in enumerate(sorted(modelrun_data.keys())):
        # and by Regionwide vs EPC
        for person_segment in ['', 'EPC ']:

            # find: header columns for this year/person segment in worksheet
            # {year} {modelrun_alias} [EPC ]Population (Thousands)
            header_text = f"{year} {modelrun_alias} {person_segment}Population (Thousands)"
            logging.debug(f"  Looking for [{header_text}]")

            found_header_text = False
            row_num = None
            col_num = None
            for row_num in range(1, sheet.max_row+1):
                for col_num in range(1, sheet.max_column+1):
                    # logging.debug(f"{row_num},{col_num}: {sheet.cell(row_num, col_num).value}")
                    if sheet.cell(row_num, col_num).value == header_text:
                        logging.debug(f"  Found at {row_num},{col_num}")
                        found_header_text = True
                        break
                # break from row loop
                if found_header_text: break
            
            if not found_header_text:
                if modelrun_alias in ["No Project","DBP"]:
                    # this is a problem -- fail hard
                    logging.fatal(f"urban_park_acres not updated for {modelrun_alias}")
                    raise Exception(f"urban_park_acres not updated for {modelrun_alias}")
                    raise("")
                else:
                    logging.info(f"  => Didn't find {header_text} -- skipping")
                    continue

            # summarize the data for this
            tazdata_df = modelrun_data[year]['TAZ1454']
            if person_segment == 'EPC ':
                tazdata_df = tazdata_df.loc[ tazdata_df.taz_epc == 1]
            
            taz_county_summary_df = tazdata_df.groupby('COUNTY').agg({'TOTPOP':'sum'})
            # convert to 1000s of population
            taz_county_summary_df['TOTPOP'] = taz_county_summary_df.TOTPOP / 1000
            logging.debug(f"  taz_county_summary_df:\n{taz_county_summary_df}")
            # fill in the workbook
            county_col_offset = -2
            if year_idx == 1: county_col_offset = -7

            for county_num in range(len(taz_county_summary_df)):
                # get county name from col-2
                county_name = sheet.cell(row_num+county_num+1, col_num + county_col_offset).value
                logging.debug(f"{county_name=} {row_num+county_num+1=} {col_num+county_col_offset=}")
                # set the population
                sheet.cell(row_num+county_num+1, col_num).value = taz_county_summary_df.loc[county_name, 'TOTPOP']
            # skip one row and fill in the modelrun_id
            sheet.cell(row_num+len(taz_county_summary_df)+2, col_num).value = modelrun_id

    # save and close the workbook
    workbook.save(DEST_WORKBOOK)
    logging.info(f"Saved {DEST_WORKBOOK}")
    workbook.close()

    # openpyxl can't make excel recalculate results; use xlwings for that
    # see https://stackoverflow.com/questions/36116162/python-openpyxl-data-only-true-returning-none/72901927#72901927
    excel_app = xlwings.App(visible=False)
    excel_book = excel_app.books.open(DEST_WORKBOOK)
    excel_book.save()
    excel_book.close()
    excel_app.quit()
    logging.info(f"Refreshed {DEST_WORKBOOK} with xlwings")


def non_greenfield_development_share(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        run_directory_path: pathlib.Path,
        output_path: pathlib.Path,
        append_output: bool
    ):
    '''
    Calculate and export the share of development that falls within the 2020 urban area footprint
    (or is outside the urban area footprint but suitably low-density as to be rural in character).
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - run_directory_path (Path): The directory path for this model run.
    - output_path (Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing.
    '''
    logging.info("Calculating non_greenfield_development_share")

    # Guard clause: this metric is implemented for RTP2025 / PBA50+ only
    if rtp != 'RTP2025':
        logging.info("  RTP2021 is not supported - skipping")
        return

    # Define a potentially very impactful constant used to convert residential units to non-residential sqft and vice versa
    SQFT_PER_UNIT = 1750  # close to the weighted average size of developer-model units in a recent BAUS run

    # Read in and select new buildings post 2020
    modelrun_name = modelrun_id
    # Sometimes the modelrun_id is a whole file path
    # Handle both forms of slashes in this field
    if '\\' in modelrun_id:
        modelrun_name = modelrun_id.split('\\')[-1]
    if '/' in modelrun_id:
        modelrun_name = modelrun_id.split('/')[-1]
    NEW_BUILDINGS_PATH = pathlib.Path(run_directory_path) / f'core_summaries/{modelrun_name}_new_buildings_summary.csv'
    logging.info(f'  Reading new_buildings_summary from {NEW_BUILDINGS_PATH}...')
    new_buildings = pd.read_csv(
        NEW_BUILDINGS_PATH,
        usecols=['parcel_id', 'year_built', 'building_sqft', 'residential_units'],
        dtype={'parcel_id': int}
    )
    new_buildings = new_buildings[new_buildings['year_built'] > 2020]
    logging.debug(f'  {len(new_buildings)} buildings built after 2020')

    # Some residential buildings (from the development pipeline) have no building_sqft);
    # convert residential units to sqft equivalent so we can summarize "all development"
    new_buildings.loc[new_buildings['building_sqft'] == 0, 'building_sqft'] = \
        new_buildings.loc[new_buildings['building_sqft'] == 0, 'residential_units'] * SQFT_PER_UNIT
    
    # We are interested in development on any parcel:
    # 1. outside the 2020 urban area footprint AND
    # 2. greater than 1 DU-equivalent per acre in 2050
    parcel_df = modelrun_data[2050]['parcel'].copy()
    parcel_df['du_equiv_per_acre'] = (parcel_df['residential_units'] + (parcel_df['non_residential_sqft'] / SQFT_PER_UNIT)) \
                                     / parcel_df['ACRES']
    dense_greenfield_parcels = parcel_df.loc[
        (parcel_df['du_equiv_per_acre'] > 1.0) & (parcel_df['in_urban_area'] == 0),
        'parcel_id'
    ]

    # Calculate share of "all development" (in terms of building_sqft) that occurred on "dense greenfield parcels"
    total_development = new_buildings['building_sqft'].sum()
    dense_greenfield_development = new_buildings.loc[
        new_buildings['parcel_id'].isin(dense_greenfield_parcels),
        'building_sqft'
    ].sum()
    greenfield_development_pct = dense_greenfield_development / total_development

    # Add metadata, format, and export to CSV
    greenfield_development_df = pd.DataFrame({
        'modelrun_id': modelrun_id,
        'modelrun_alias': modelrun_alias,
        'area_alias': 'Regionwide',
        'area': 'all',
        'development_in_urban_footprint_pct': 1 - greenfield_development_pct
    }, index=[0])
    out_file = pathlib.Path(output_path) / 'metrics_healthy2_development_in_urban_footprint.csv'
    greenfield_development_df.to_csv(
        out_file,
        mode='a' if append_output else 'w',
        header=False if append_output else True,
        index=False,
    )
    logging.info(f"{'Appended' if append_output else 'Wrote'} {len(greenfield_development_df)} " \
                 + f"line{'s' if len(greenfield_development_df) > 1 else ''} to {out_file}")
    
    
def slr_protection(rtp, modelrun_alias, modelrun_id, modelrun_data, output_path, append_output):
    """
    Calculates the percentage of households that are protected by sea level rise mitigation, 
    as a percentage of all households in sea level rise areas and a percentage of all 
    households in sea level rise areas that are EPCs.

    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame}
    - output_path (str): File path for saving the output results
    - append_output (bool): True if appending output; False if writing

    Writes metrics_slrProtection.csv to output_path, appending if append_output is True. Columns are:
    - modelrun_alias
    - hazard
    - area_alias
    - area
    - protected_households_pct
    - hazard_alias
    """
    logging.info("Calculating slr_protection")

    # get parcel data for the horizon year
    year_horizon = sorted(modelrun_data.keys())[-1]
    df = modelrun_data[year_horizon]["parcel"]

    this_modelrun_alias = metrics_utils.classify_runid_alias(modelrun_alias)

    geog_name = 'eir_coc_id' if rtp=="RTP2021" else 'epc_id'

    # SLR parcels - all parcels in the SLR input files that are inundated or mitigated
    slr_area = [df.inundation.isin([12,24,10,20,100]), (df.inundation.isin([12,24,10,20,100]) & (df[geog_name].notnull()))]
    slr_protected_area = [df.inundation == 100, (df.inundation == 100) & (df[geog_name].notnull())]

    protected_households_pct = []
    for slr, slr_protected in zip(slr_area, slr_protected_area):
        # households on SLR parcels
        slr_households = df.loc[slr]['tothh'].sum()
        logging.debug("{} SLR households".format(slr_households))

        # in the No Project case where no parcels experience SLR, call them all "unprotected"
        if (slr_households == 0) & (this_modelrun_alias == "NP"):
            protected_pct = 0
        # in the Project case where no parcels experience SLR, call them all "protected"
        elif slr_households == 0:   
            protected_pct = 1
        # otherwise calculate the percent protected normally
        else:
            protected_households = df.loc[slr_protected]['tothh'].sum()
            logging.debug("{} total protected SLR households".format(protected_households))
            protected_pct =  protected_households / slr_households
        # total households on SLR parcels that are protected
        protected_households_pct.append(protected_pct)
        logging.debug("percent total protected households is {}".format(protected_pct))
    
    df = pd.DataFrame({})
    df['area'] = ['all', 'COC' if rtp=='RTP2021' else 'EPC']
    df['area_alias'] = ['All Households', 'Communities of Concern' if rtp=='RTP2021' else 'Equity Priority Communities']
    df['hazard'] = 'SLR'
    df['hazard_alias'] = 'Sea Level Rise'
    df['protected_households_pct'] = protected_households_pct
    df['modelrun_id'] = modelrun_id
    df['modelrun_alias'] = modelrun_alias
    # make column ordering match template
    if rtp=='RTP2021':
        slr_protection_df = df[['modelrun_alias', 'hazard', 'area_alias', 'area', 'protected_households_pct', 'hazard_alias']]
    else:
        df['modelrun_id'] = modelrun_id
        slr_protection_df = df[['modelrun_id', 'modelrun_alias', 'hazard', 'area_alias', 'area', 'protected_households_pct', 'hazard_alias']]
        
    filename = "metrics_healthy1_hazard_resilience_SLR.csv"
    filepath = output_path / filename

    slr_protection_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(df), filepath))
