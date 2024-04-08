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
    - modelrun_id
    - modelrun_alias
    - area
    - total_households
    - q1_households
    - q1_household_share
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

def gentrify_displacement_tracts(
    rtp: str,
    modelrun_alias: str,
    modelrun_id: str,
    modelrun_data: dict,
    output_path: str,
    append_output: bool 
):
    """
    Calculates 
    1) displacement, defined as net loss of low income households in a census tract between the
       initial and horizon year, and
    2) gentrification, defined as over 10% drop in share of low income households in a census tract between
       the initial and horizon year.
    Both of these are calculated by 
      a) region, EPC and High Displacement Risk, and also 
      b) within growth geographies, within GG & HRA, and within GG and TRAs.

    Args:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str): File path for saving the output results
    - append_output (bool): True if appending output; False if writing

    Writes metrics_diverse2_gentrify_displacement_tracts.csv to output_path, appending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - area_category1
    - area_category2
    - [displacement|gentrification]_tracts[_share]
    - all_tracts
    """
    logging.info("Calculating gentrify_displacement_tracts")
    
    SUMMARY_YEARS = sorted(modelrun_data.keys())

    INITIAL_YEAR = SUMMARY_YEARS[0]
    HORIZON_YEAR = SUMMARY_YEARS[-1]

    tract_summary_df = None
    for year in SUMMARY_YEARS:
        # summarize to tract_id and the tract-level variables
        tract_summary_year_df = modelrun_data[year]['parcel'].groupby([
            'tract_id','tract_epc','tract_DispRisk','tract_hra','tract_growth_geo','tract_tra']).aggregate({
                'hhq1' :'sum',
                'tothh':'sum',
            })
        tract_summary_year_df['hhq1_share'] = tract_summary_year_df.hhq1 / tract_summary_year_df.tothh
        logging.debug('tract_summary_year_df {:,} rows:\n{}'.format(len(tract_summary_year_df), tract_summary_year_df))

        if year == INITIAL_YEAR:
            tract_summary_df = tract_summary_year_df
        else: # merge
            tract_summary_df = pd.merge(
                left        = tract_summary_df,
                right       = tract_summary_year_df,
                how         = 'outer',
                right_index = True,
                left_index  = True,
                suffixes    = (f"_{INITIAL_YEAR}", f"_{HORIZON_YEAR}"),
                validate    = 'one_to_one'
            )
    logging.debug('tract_summary_df:\n{}'.format(tract_summary_df))
    # displacement, defined as net loss of low income households in a census tract between the initial and horizon year
    tract_summary_df['displacement'] = False
    tract_summary_df.loc[ tract_summary_df[f'hhq1_{HORIZON_YEAR}'] < tract_summary_df[f'hhq1_{INITIAL_YEAR}'], 
                         'displacement' ] = True
    # gentrification, defined as over 10% drop in share of low income households in a census tract between 
    # the initial and horizon year
    tract_summary_df['gentrification'] = False
    tract_summary_df.loc[ tract_summary_df[f'hhq1_share_{HORIZON_YEAR}']/tract_summary_df[f'hhq1_share_{INITIAL_YEAR}'] < 0.9, 
                         'gentrification'] = True

    # reset index. columns are now: 
    #   tract_id  tract_epc  tract_DispRisk  tract_hra  tract_growth_geo  tract_tra
    #   hhq1_[initial_year]  tothh_[horizon_year]  hhq1_share_[initial_year]
    #   hhq1_[horizon_year]  tothh_[horizon_year]  hhq1_share_[horizon_year]
    #   displacement  gentrification
    tract_summary_df.reset_index(drop=False, inplace=True)
    logging.debug('tract_summary_df:\n{}'.format(tract_summary_df))

    # a) region, EPC and HRA, and also 
    # b) within growth geographies, within GG & HRA, and within GG and TRAs.
    summary_dict_list = []
    area_categories1 = ['Region','GrowthGeography']
    for area_category1 in area_categories1:

        # filter to area_category1
        if area_category1 == 'Region':
            tract_summary_category1_df = tract_summary_df
        elif area_category1 == 'GrowthGeography':
            tract_summary_category1_df = tract_summary_df.loc[ tract_summary_df.tract_growth_geo == 1]

        # define area_categories2
        area_categories2 = ['all', 'EPC', 'DispRisk'] if area_category1 == 'Region' else ['all', 'HRA','TRA']
        for area_category2 in area_categories2:

            # filter to area_category2
            if area_category2 == 'all':
                tract_summary_category1_2_df = tract_summary_category1_df # no filter
            elif area_category2 == 'EPC':
                tract_summary_category1_2_df = tract_summary_category1_df.loc[ tract_summary_category1_df.tract_epc == 1]
            elif area_category2 == 'DispRisk':
                tract_summary_category1_2_df = tract_summary_category1_df.loc[ tract_summary_category1_df.tract_DispRisk == 1]
            elif area_category2 == 'HRA':
                tract_summary_category1_2_df = tract_summary_category1_df.loc[ tract_summary_category1_df.tract_hra == 1]
            elif area_category2 == 'TRA':
                tract_summary_category1_2_df = tract_summary_category1_df.loc[ tract_summary_category1_df.tract_tra == 1]

            # summarize and gentrification
            summary_dict = {
                'area_category1': area_category1,
                'area_category2': area_category2,
            }
            for summarize_var in ['displacement','gentrification']:
                summary_df = tract_summary_category1_2_df.groupby([summarize_var]).agg(
                    tract_count     =pd.NamedAgg(column="tract_id",   aggfunc="count"),
                )
                # this is a DataFrame with index named summarize_var, 1 column named 'tract_count'
                # and 1-2 rows
                tract_count_var = summary_df.loc[ True, 'tract_count']
                tract_count_all = summary_df['tract_count'].sum()
                logging.debug(f'summary_df for cat1={area_category1} cat2={area_category2}: ' \
                              'tract_count_var={tract_count_var} tract_count_all={tract_count_all}')

                # save it to summary_dict
                summary_dict[f'{summarize_var}_tracts'      ] = tract_count_var
                summary_dict['all_tracts'                   ] = tract_count_all # yes this is done twice but the result is the same
                summary_dict[f'{summarize_var}_tracts_share'] = tract_count_var/tract_count_all
            # add to our list
            summary_dict_list.append(summary_dict)

    summary_df = pd.DataFrame(summary_dict_list)
    # add columns
    summary_df.insert(0, 'modelrun_id', modelrun_id)
    summary_df.insert(0, 'modelrun_alias', f'{HORIZON_YEAR} {modelrun_alias}')

    logging.debug(f"Final summary_df:\n{summary_df}")

    # save this
    filename = "metrics_diverse2_gentrify_displacement_tracts.csv"
    filepath = output_path / filename
    summary_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(summary_df), filepath))
