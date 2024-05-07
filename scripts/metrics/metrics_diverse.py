# ==============================
# ====== Diverse Metrics =======
# ==============================

import pandas as pd
import logging, pathlib
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

    SUMMARIZATION_CATEGORIES = [
        ('Region',          'all_region'), # Region != all_region for tableau aliasing 
        ('Region',          'EPC'       ),
        ('Region',          'DispRisk'  ),
        ('GrowthGeography', 'all_gg'    ), # GrowthGeography != all_gg for tableau aliasing
        ('GrowthGeography', 'HRA'       ),
        ('GrowthGeography', 'TRA'       )
    ]

    CATEGORY_TO_RTP_TRACT_ID = {
        # for RTP2021/PBA50, all tract lookups are relative to tract10
        'RTP2021':{
            'Region':           None,
            'all_region':       'tract10',
            'EPC':              'tract10_epc',
            'DispRisk':         'tract10_DispRisk',
            'GrowthGeography':  'tract10_growth_geo',
            'all_gg':           None, # no additional filter
            'HRA':              'tract10_hra',
            'TRA':              'tract10_tra',
        },
        # for RTP2025/PBA50+, Displacement Risk is relative to tract10, others are relative to tract20
        'RTP2025':{
            'Region':           None,
            'all_region':       'tract20',
            'EPC':              'tract20_epc',
            'DispRisk':         'tract10_DispRisk',
            'GrowthGeography':  'tract20_growth_geo',
            'all_gg':           None, # no additional filter
            'HRA':              'tract20_hra',
            'TRA':              'tract20_tra',
        }
    }
    # figure out tract20 and tract10 keys
    tract_keys = { 'tract10':set(['tract10']), 'tract20':set(['tract20'])}
    for category_tuple in SUMMARIZATION_CATEGORIES:
        for cat in category_tuple:
            if CATEGORY_TO_RTP_TRACT_ID[rtp][cat] == None: continue
            if CATEGORY_TO_RTP_TRACT_ID[rtp][cat].startswith('tract10'):
                tract_keys['tract10'].add(CATEGORY_TO_RTP_TRACT_ID[rtp][cat])
            if CATEGORY_TO_RTP_TRACT_ID[rtp][cat].startswith('tract20'):
                tract_keys['tract20'].add(CATEGORY_TO_RTP_TRACT_ID[rtp][cat])
    logging.debug(f"{tract_keys=}")

    # store results here to build dataframe
    summary_dict_list = []
    for tract_id in tract_keys.keys():
        if len(tract_keys[tract_id]) == 1: continue

        logging.debug(f"Processing tract_id {tract_id}; tract_keys={tract_keys[tract_id]}")

        for year in SUMMARY_YEARS:
            # summarize to tract_id and the tract-level variables
            tract_summary_year_df = modelrun_data[year]['parcel'].groupby(
                sorted(list(tract_keys[tract_id]))).aggregate({
                'hhq1' :'sum',
                'tothh':'sum',
            })
            tract_summary_year_df['hhq1_share'] = tract_summary_year_df.hhq1 / tract_summary_year_df.tothh
            logging.debug('tract_summary_year_df by {} {:,} rows:\n{}'.format(
                tract_id, len(tract_summary_year_df), tract_summary_year_df))

            # merge years
            if year == INITIAL_YEAR:
                multiyear_tract_summary_df = tract_summary_year_df
            else: # merge
                multiyear_tract_summary_df = pd.merge(
                left        = multiyear_tract_summary_df,
                right       = tract_summary_year_df,
                how         = 'outer',
                right_index = True,
                left_index  = True,
                suffixes    = (f"_{INITIAL_YEAR}", f"_{HORIZON_YEAR}"),
                validate    = 'one_to_one'
            )

        # displacement, defined as net loss of low income households in a census tract between the initial and horizon year
        multiyear_tract_summary_df['displacement'] = (
            multiyear_tract_summary_df[f'hhq1_{HORIZON_YEAR}'] < multiyear_tract_summary_df[f'hhq1_{INITIAL_YEAR}']).fillna(False)
        # gentrification, defined as over 10% drop in share of low income households in a census tract between
        # the initial and horizon year
        multiyear_tract_summary_df['gentrification'] = (
            multiyear_tract_summary_df[f'hhq1_share_{HORIZON_YEAR}']/multiyear_tract_summary_df[f'hhq1_share_{INITIAL_YEAR}'] < 0.9).fillna(False)

        multiyear_tract_summary_df['lihh_change'] = (
            multiyear_tract_summary_df[f'hhq1_{HORIZON_YEAR}'] - multiyear_tract_summary_df[f'hhq1_{INITIAL_YEAR}']).fillna(False)
        
        logging.debug('multiyear_tract_summary_df.lihh_change:\n{}'.format(multiyear_tract_summary_df.lihh_change.describe()))
        # reset index. columns are now:
        #   tract[10|20] [tract10|20 keys]
        #   hhq1_[initial_year]  tothh_[horizon_year]  hhq1_share_[initial_year]
        #   hhq1_[horizon_year]  tothh_[horizon_year]  hhq1_share_[horizon_year]
        #   displacement  gentrification
        multiyear_tract_summary_df.reset_index(drop=False, inplace=True)
        logging.debug('multiyear_tract_summary_df:\n{}'.format(multiyear_tract_summary_df))

        # for debugging
        # add columns
        multiyear_tract_summary_df.insert(0, 'modelrun_id', modelrun_id)
        multiyear_tract_summary_df.insert(0, 'modelrun_alias', f'{HORIZON_YEAR} {modelrun_alias}')

        filename = f"debug_gentrify_displacment_{tract_id}.csv"
        filepath = output_path / filename
        multiyear_tract_summary_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
        logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(multiyear_tract_summary_df), filepath))

        # create the relevant results tally
        for (cat1,cat2) in SUMMARIZATION_CATEGORIES:

            cat1_tract_var = CATEGORY_TO_RTP_TRACT_ID[rtp][cat1]
            cat2_tract_var = CATEGORY_TO_RTP_TRACT_ID[rtp][cat2]
            logging.debug(f"Summarizing ({cat1},{cat2}); {cat1_tract_var=} {cat2_tract_var=}")
            # error if both not none and mismatching (e.g. can't summarize on tract10 and tract20 at the same time)
            if cat1_tract_var and cat2_tract_var:
                # logging.debug(f"{cat1_tract_var[:7]} == {cat2_tract_var[:7]}")
                assert(cat1_tract_var[:7] == cat2_tract_var[:7])
            # if this summary doesn't match with tract_id -- continue
            # e.g. if tract_id==tract10 but this is a tract20 summary
            if cat1_tract_var and cat1_tract_var.startswith(tract_id) == False: continue
            if cat2_tract_var and cat2_tract_var.startswith(tract_id) == False: continue

            # filter to cat1
            if cat1 == 'Region':
                category_tract_summary_df = multiyear_tract_summary_df # no filter
            elif cat1 == 'GrowthGeography':
                category_tract_summary_df = multiyear_tract_summary_df.loc[multiyear_tract_summary_df[cat1_tract_var] == 1]  # tract[10|20]_growth_geo == 1
            else:
                raise RuntimeError(f"{cat1=} not supported")

            # filter to cat2
            if cat2.startswith('all'):
                pass # no filter
            else:
                category_tract_summary_df = category_tract_summary_df.loc[category_tract_summary_df[cat2_tract_var] == 1]

            logging.debug(f"  category_tract_summary_df.head():\n{category_tract_summary_df.head()}")
            # columns:

            # summarize displacement and gentrification
            tract_count_all = len(category_tract_summary_df)
            summary_dict = {
                'area_category1': cat1,
                'area_category2': cat2,
                'all_tracts': tract_count_all,
            }
            for summarize_var in ['displacement','gentrification']:
                tract_count_var = category_tract_summary_df[summarize_var].sum()  # sum coerces booleans to ints 0/1
                logging.debug(f'  {summarize_var} for {cat1=} {cat2=}: {tract_count_var=} {tract_count_all=}')

                # save it to summary_dict
                summary_dict[f'{summarize_var}_tracts'      ] = tract_count_var
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


def lowinc_homeownership_share(
            rtp: str,
            modelrun_alias: str,
            modelrun_id: str,
            m_path: pathlib.Path,
            box_path: pathlib.Path,
            output_path: str,
            append_output: bool
        ) -> pd.DataFrame:
    """
    Calculates the future share of low-income households owning a home in the Bay Area.

    This function retrieves data from various sources and applies assumptions from RTP2021
    to estimate the future ownership rate for low-income households. There is no variation
    between DBP runs - this is based on regional data.

    Args:
        rtp: RTP scenario name (string)
        modelrun_alias: Model run alias (string)
        modelrun_id: Model run ID (string)
        m_path: Path to model data directory (Path or string)
        box_path: Path to box drive (Path or string)
        output_path: Path to save the output (string)
        append_output: Flag indicating if output should be appended (bool)

    Writes the file: metrics_diverse1_lowincome_homeownership.csv to output_path, 
    appending if append_output is True. Columns are:
    - modelrun_id
    - modelrun_alias
    - Home Ownership Rate _ Low Income
    - name
    """
    logging.info("Calculating lowinc_homeownership_share")
    if rtp == "RTP2021":
        logging.info(f"   {rtp} not supported - skipping")
        return

    # TODO: consider if we should just have a dict for this - there is no variation between project runs - we could run once and lookup later.
    import pathlib

    # relates RTP scenario to the variant shortname - getting the proper matching control total series
    variant_mapping = {
        'NoProject':'NP',
        'NP':'NP',
        'No Project':'NP',
        'DBP':'DBP',
        'Draft Blueprint':'DBP'
     }

    def pct(x): return x / x.sum()


    # this is the number of units assumed to switch from rental to ownership
    UNIT_SUPPORT_CONSTANT = 100000

    unit_support_contant_dbp_s = pd.Series(UNIT_SUPPORT_CONSTANT, index=['DBP'])
    unit_support_contant_dbp_s.index = unit_support_contant_dbp_s.index.set_names(
        'variant')

    unit_support_contant_np_s = pd.Series(0, index=['NP'])
    unit_support_contant_np_s.index = unit_support_contant_np_s.index.set_names(
        'variant')

    logging.debug("   Creating unit support Series from constant...")
    unit_support_contant_s = pd.concat(
        [unit_support_contant_np_s, unit_support_contant_dbp_s])

    # Control totals
    logging.debug("   Reading control totals data...")

    control_totals_path_dbp = m_path / 'urban_modeling' / 'baus' / 'BAUS Inputs' / \
        'regional_controls'/'household_controls_PBA50Plus_DBP_UBI2030.csv'
    control_totals_path_np = m_path / 'urban_modeling'/'baus'/'BAUS Inputs' / \
        'regional_controls'/'household_controls_PBA50Plus_np.csv'

    reg_forecast_hh_dbp = pd.read_csv(
        control_totals_path_dbp, index_col=0)
    reg_forecast_hh_dbp_q1 = reg_forecast_hh_dbp.q1_households

    reg_forecast_hh_np = pd.read_csv(
        control_totals_path_np, index_col=0)
    reg_forecast_hh_np_q1 = reg_forecast_hh_np.q1_households

    # combine controls in one series
    controls_q1 = pd.concat([reg_forecast_hh_np_q1, reg_forecast_hh_dbp_q1], keys=[
                            'NP', 'DBP'], names=['variant', 'year'])


    logging.debug(f"   DBP has {reg_forecast_hh_dbp_q1[2050]} households in 2050")
    logging.debug(f"   NP has {reg_forecast_hh_np_q1[2050]} households in 2050")


    # Income by tenure from PUMS
    logging.debug("   Reading income by tenure data from PUMS...")

    tenure_by_income_path = box_path / 'Plan Bay Area 2050+' / 'Performance and Equity' / 'Plan Performance' / \
        'Equity_Performance_Metrics' / 'Draft_Blueprint' / 'metrics_input_files' / \
        'pums_2019_2021_tenure_by_1999_income_quartile.csv'
    
    tenure_by_income = pd.read_csv(
        tenure_by_income_path, index_col=[0, 1, 2, 3]).WGTP

    logging.debug("   Calculating household tenure by income percentage...")

    # we do mean here, omitting the year dimension - so we average 2019, 2021 observations
    hh_ten_by_inc_pct = (tenure_by_income
                        .groupby(level=['ten', 'incvar_vintage', 'hinc00_cat']).mean(
                        )
                        .groupby(level=['incvar_vintage', 'hinc00_cat'], group_keys=False)
                        .apply(pct))

    logging.debug(f'   head of tenure by income\n{hh_ten_by_inc_pct.head()}')
    baseyear_q1_ownership_share = hh_ten_by_inc_pct.loc['own', 'hinc99', 'HHINCQ1']
    logging.debug(f'   just the relevant q1 ownership share {baseyear_q1_ownership_share:.2f}')


    # multiplying with ownership share for q1
    future_q1_ownership_households = controls_q1.mul(
        baseyear_q1_ownership_share).round(0).astype(int)

    logging.debug(f'   head of ownership share {baseyear_q1_ownership_share}')

    future_q1_ownership_households_w_support = future_q1_ownership_households.add(
        unit_support_contant_s)  # .loc[:,2050]
    logging.debug(f'   head of ownership share after adding {UNIT_SUPPORT_CONSTANT} units:\n{future_q1_ownership_households_w_support.head()}')

    result_combo = future_q1_ownership_households_w_support.div(controls_q1).round(3)
    logging.debug(f'   head of the resulting shares {result_combo.dropna().head()}')

    # get the result share for just this modelrun (e.g. NP or DBP)
    #this_modelrun_alias = metrics_utils.classify_runid_alias(modelrun_alias)
    

    this_result = result_combo[variant_mapping[modelrun_alias]][2050]

    # collect results with relevant identifiers
    results_df = [{
        'modelrun_id': modelrun_id,
        'modelrun_alias': f"2050 {modelrun_alias}",
        'Home Ownership Rate _ Low Income': this_result,
        'name': 'Regionwide'}]

    results_df = pd.DataFrame(results_df)

    # save this
    filename = "metrics_diverse1_lowincome_homeownership.csv"
    filepath = output_path / filename
    results_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(results_df), filepath))
