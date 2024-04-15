# ==============================
# ====== Healthy Metrics =======
# ==============================

import logging, pathlib, shutil
import pandas as pd
import openpyxl
import xlwings

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

    SUMMARY_YEARS = sorted(modelrun_data.keys())
    # TODO: Right now we'll pretend 2020 data is really 2023
    # TODO: See task: Make 2023 baseyear (rather than 2020), use NoProject run for it, and use it 
    # TODO:           for all scenario initial year data
    # TODO:           https://app.asana.com/0/0/1207031046044722/f
    WORKSHEET_SUMMARY_YEARS = [2023 if year==2020 else year for year in SUMMARY_YEARS]

    # summarize by year
    for year_idx in range(len(SUMMARY_YEARS)):
        year = WORKSHEET_SUMMARY_YEARS[year_idx]
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
                logging.info(f"  => Didn't find {header_text} -- skipping")
                continue

            # summarize the data for this
            tazdata_df = modelrun_data[SUMMARY_YEARS[year_idx]]['TAZ1454']
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
                # logging.debug(f"county_name={county_name}")
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
