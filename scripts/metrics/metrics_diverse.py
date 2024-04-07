# ==============================
# ====== Diverse Metrics =======
# ==============================

import pandas as pd
import logging
import metrics_utils

def low_income_households_share(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        output_path: str,
        append_output: bool
    ):
    """
    Calculate the share of households that are low-income in Transit-Rich Areas (TRA), High-Resource Areas (HRA), and both
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str): File path for saving the output results
    - append_output (bool): True if appending output; False if writing

    Writes metrics_diverse1_q1_hh_share.csv, appending if append_output is True. Columns are:
    """ 
    logging.info("Calculating low_income_households_share")
    
    SUMMARY_YEARS = sorted(modelrun_data.keys())
    summary_list = []
    # Process each area and year
    for year in SUMMARY_YEARS:
        for area in ['HRA','TRA','HRAandTRA','EPC','Region']:
            filter_condition = metrics_utils.PARCEL_AREA_FILTERS[rtp][area]
            if callable(filter_condition):  # Check if the filter is a function
                df_area = modelrun_data[year]['parcel'].loc[filter_condition(modelrun_data[year]['parcel'])]
            elif filter_condition == None:
                df_area = modelrun_data[year]['parcel']
            logging.debug("area={} df_area len={:,}".format(area, len(df_area)))

            # Calculate the share and append to results
            summary_list.append({
                'modelrun_id'             : modelrun_id,
                'modelrun_alias'          : f"{year} {modelrun_alias}",
                'area'                    : area,
                'total_households'        : df_area['tothh'].sum(),
                'q1_households'           : df_area['hhq1'].sum(),
                'q1_household_share'      : df_area['hhq1'].sum() / df_area['tothh'].sum()
            })

    # Create the results DataFrame
    hh_share_df = pd.DataFrame(summary_list)

    filename = "metrics_diverse1_q1_hh_share.csv"
    filepath = output_path / filename

    hh_share_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(hh_share_df), filepath))