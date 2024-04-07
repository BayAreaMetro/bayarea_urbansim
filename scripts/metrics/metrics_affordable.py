# ==============================
# ===== Affordable Metrics =====
# ==============================

import logging
import pandas as pd
import metrics_utils

OFFMODEL = {
    # Hard-coded numbers for off-model units from plan PBA50. 
    'RTP2021':{
        'PRESERVED': 40000,
        'HOMELESS' : 35000,
        'PIPELINE' : 7572
    },
    # Mark: We no longer use them for PBA50Plus metric calculations because:
    #   Preserved is integrated into the model input provided for H2 (i.e., the preserved units targets by geography combination) 
    #   Homeless is integrated into the model input provided for H4, similar to H2 above. 
    #   It is assumed that 'pipeline' units are already incorporated in the pipeline data.
    'RTP2025':{
        'PRESERVED': 0,
        'HOMELESS' : 0,
        'PIPELINE' : 0
    }
}

def deed_restricted_affordable_share(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        output_path: str,
        append_output: bool
    ):
    """
    Calculate the share of housing that is deed-restricted affordable at the regional level, 
    within Equity Priority Communities (EPC), and within High-Resource Areas (HRA)
    
    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing

    Writes metrics_affordable2_deed_restricted_pct.csv to output_path, appending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - deed_restricted_units
    - preserved_units
    - residential_units
    - deed_restricted_pct: deed_restricted_units/residential_units

    Also writes metrics_affordable2_newUnits_deed_restricted_pct.csv to output_path, appending if append_output is True. Columns are:
    - modelrun_alias
    - modelrun_id
    - area
    - [deed_restricted_units|preserved_units|residential_units]_[year]: types of units existing in the given year
    - deed_restricted_produced: deed_restricted_units - preserved_units, since preserved aren't produced
    - [deed_restricted_prod|residential_units]_diff: change in deed restricted/residential units
    - deed_restricted_pct_new_units: what percent of residential units created were deed restricted

    """
    logging.info("Calculating deed_restricted_affordable_share")

    SUMMARY_YEARS = sorted(modelrun_data.keys())
    INITIAL_YEAR = SUMMARY_YEARS[0]
    HORIZON_YEAR = SUMMARY_YEARS[-1]

    summary_list = [] # list of dicts
    for year in SUMMARY_YEARS:
        for area in ['HRA','EPC','Region']:
            filter_condition = metrics_utils.PARCEL_AREA_FILTERS[rtp][area]
            if callable(filter_condition):
                df_area = modelrun_data[year]['parcel'].loc[filter_condition(modelrun_data[year]['parcel'])]
            elif filter_condition == None:
                df_area = modelrun_data[year]['parcel']
            logging.debug("area={} df_area len={:,}".format(area, len(df_area)))

            deed_restricted_units = df_area['deed_restricted_units'].sum()
            residential_units     = df_area['residential_units'].sum()
            preserved_units       = df_area['preserved_units'].sum()
            if area=='Region':
                if year == HORIZON_YEAR:
                    # RTP2021: assume additional deed restricted units created: preserved and pipeline
                    deed_restricted_units = deed_restricted_units + OFFMODEL[rtp]['HOMELESS'] + \
                         OFFMODEL[rtp]['PRESERVED'] + OFFMODEL[rtp]['PIPELINE']
                    residential_units     = residential_units     + OFFMODEL[rtp]['HOMELESS']
                    preserved_units       = preserved_units       + OFFMODEL[rtp]['PRESERVED']
            # assume preserved units are all in EPC
            if (area=="EPC") and (year==HORIZON_YEAR):
                deed_restricted_units = deed_restricted_units + OFFMODEL[rtp]['PRESERVED']
                preserved_units       = preserved_units       + OFFMODEL[rtp]['PRESERVED']

            # note: Per original script, for PBA50, we need to subtract out preserved_units_2015, because urbansim adds preserved units 
            # as a result of PBA strategies between 2010 and 2015. This is because  the strategy was not coded to be "smart" 
            # and add units only after 2015 - source: Elly            
            if year == INITIAL_YEAR:
                deed_restricted_units = deed_restricted_units - preserved_units

            summary_list.append({
                'modelrun_id'             : modelrun_id,
                'modelrun_alias'          : f"{year} {modelrun_alias}",
                'year'                    : year,
                'area'                    : area,
                'deed_restricted_units'   : deed_restricted_units,
                'preserved_units'         : preserved_units,
                'residential_units'       : residential_units,
                'deed_restricted_produced': deed_restricted_units - preserved_units,  # produced = not preserved
                # share of all units
                'deed_restricted_share_of_all_units' : (deed_restricted_units / residential_units) if residential_units > 0 else 0,
            })

    # Create the results DataFrame
    summary_df = pd.DataFrame(summary_list)
    logging.debug("summary_df:\n{}".format(summary_df))

    filename = "metrics_affordable2_deed_restricted_pct.csv"
    filepath = output_path / filename

    summary_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(summary_df), filepath))

    # part two - new units
    # Doing this with the same code because it depends on the above code, and repeating it doesn't make sense
    summary_initial_df = summary_df.loc[summary_df.year == INITIAL_YEAR, ['modelrun_id','area','deed_restricted_units','preserved_units','residential_units','deed_restricted_produced']]
    summary_horizon_df = summary_df.loc[summary_df.year == HORIZON_YEAR, ['modelrun_id','area','deed_restricted_units','preserved_units','residential_units','deed_restricted_produced']]
    logging.debug("summary_initial_df:\n{}".format(summary_initial_df))
    summary_wide_df = pd.merge(
        left     = summary_initial_df,
        right    = summary_horizon_df,
        on       = ['modelrun_id','area'],
        how      = 'left',
        suffixes = [f'_{INITIAL_YEAR}',f'_{HORIZON_YEAR}'],
        validate = 'one_to_one'
    )
    # growth in deed restricted units and residential units overall
    summary_wide_df['deed_restricted_prod_diff'] = summary_wide_df[f'deed_restricted_produced_{HORIZON_YEAR}'] - \
                                                   summary_wide_df[f'deed_restricted_produced_{INITIAL_YEAR}']
    summary_wide_df['residential_units_diff']    = summary_wide_df[f'residential_units_{HORIZON_YEAR}'] - \
                                                   summary_wide_df[f'residential_units_{INITIAL_YEAR}']
    # hoe much of the residential unit growth is deed restricted?
    summary_wide_df['deed_restricted_pct_new_units'] = summary_wide_df['deed_restricted_prod_diff'] / summary_wide_df['residential_units_diff']

    # add modelrun_alias as first column
    summary_wide_df.insert(0, 'modelrun_alias', modelrun_alias)

    # save this
    filename = "metrics_affordable2_newUnits_deed_restricted_pct.csv"
    filepath = output_path / filename
    summary_wide_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(summary_wide_df), filepath))

def at_risk_housing_preserve_share(
        horizon_year: int,
        modelrun_alias: str,
        modelrun_id: str,
        output_path: str,
        append_output: bool
    ):
    """
    Creates a DataFrame that indicates the percentage of at-risk preservation for a specific model run.
    Depending on the model run alias, this function will assign a value to the 'at_risk_preserv_pct' field:
    1 if the model run alias is "No Project", otherwise 0. 

    Parameters:
    - modelrun_id (str): The identifier for the model run.
    - modelrun_alias (str): The alias for the model run.
    - output_path (str): The file path where the resulting DataFrame will be saved.
    
    Writes metrics_affordable2_at_risk_housing_preserve_pct.csv with columns:
    - modelrun_alias
    - modelrun_id
    - area = Region
    - at_risk_preserve_pct
    """
    value = 0 if modelrun_alias in ["No Project"] else 1

    results = [{
        'modelrun_id'          : modelrun_id,
        'modelrun_alias'       : f"{horizon_year} {modelrun_alias}",
        'area'                 : 'Region',
        'at_risk_preserve_pct' : value
    }]
    
    # Convert the list of dictionaries into a pandas DataFrame
    at_risk_df = pd.DataFrame(results)

    # write it
    filename = "metrics_affordable2_at_risk_housing_preserve_pct.csv"
    filepath = output_path / filename
    at_risk_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(at_risk_df), filepath))
