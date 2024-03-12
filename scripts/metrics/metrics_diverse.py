# ==============================
# ====== Diverse Metrics =======
# ==============================

from datetime import datetime
import logging

def low_income_households_share(parcel_geog_summary_2020, parcel_geog_summary_2050, modelrun_id, model_run_alias, pba50plus_path):
    """
    Calculate the share of households that are low-income in Transit-Rich Areas (TRA), High-Resource Areas (HRA), and both
    """
    def calculate_share_low_inc(df_2020, df_2050):
        total_hhs_2020 = df_2020['tothh'].sum()
        total_hhs_2050 = df_2050['tothh'].sum()
        low_inc_hhs_2020 = df_2020['hhq1'].sum()
        low_inc_hhs_2050 = df_2050['hhq1'].sum()
        low_inc_hh_share_2020 = round((low_inc_hhs_2020 / total_hhs_2020), 2) if total_hhs_2020 > 0 else 0
        low_inc_hh_share_2050 = round((low_inc_hhs_2050 / total_hhs_2050), 2) if total_hhs_2050 > 0 else 0
        
        return low_inc_hh_share_2020, low_inc_hh_share_2050
    
    # Overall calculation
    overall_low_inc_hh_share = calculate_share_low_inc(parcel_geog_summary_2020, parcel_geog_summary_2050)

    # Calculation for HRA
    hra_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['hra_id'] == 'HRA']
    hra_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['hra_id'] == 'HRA']
    hra_low_inc_hh_share = calculate_share_low_inc(hra_filter_2020, hra_filter_2050)

    # Calculation for TRA
    tra_filter_2020 = parcel_geog_summary_2020[parcel_geog_summary_2020['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])]
    tra_filter_2050 = parcel_geog_summary_2050[parcel_geog_summary_2050['tra_id'].isin(['TRA1', 'TRA2', 'TRA3'])]
    tra_low_inc_hh_share = calculate_share_low_inc(tra_filter_2020, tra_filter_2050)

    # Calculation for both TRA and HRA
    tra_hra_filter_2020 = parcel_geog_summary_2020[(parcel_geog_summary_2020['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']))&(parcel_geog_summary_2020['hra_id'] == 'HRA')]
    tra_hra_filter_2050 = parcel_geog_summary_2050[(parcel_geog_summary_2050['tra_id'].isin(['TRA1', 'TRA2', 'TRA3']))&(parcel_geog_summary_2050['hra_id'] == 'HRA')]
    tra_hra_low_inc_hh_share = calculate_share_low_inc(tra_hra_filter_2020, tra_hra_filter_2050)

    # Define the metrics and years for iteration
    metrics_names = ["low_income_households_share_total",
                     "low_income_households_share_hra",
                     "low_income_households_share_tra",
                     "low_income_households_share_tra_hra"]
    metrics_values = [overall_low_inc_hh_share, hra_low_inc_hh_share, tra_low_inc_hh_share, tra_hra_low_inc_hh_share]
    metrics_years = [2020, 2050]
    result_hh_share = assemble_results_wide_format(modelrun_id, 'D1', metrics_names, metrics_years, model_run_alias, metrics_values)
    # Call the utility function to save the results
    filename = f"metrics_{modelrun_id}_{model_run_alias}_low_income_households_share_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
    filepath = pba50plus_path / "Metrics" / filename
    result_hh_share.to_csv(filepath, index=False)
    logging.info(f"Saved metric results to {filepath}")
    return result_hh_share
