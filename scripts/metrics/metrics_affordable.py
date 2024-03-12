# ==============================
# ===== Affordable Metrics =====
# ==============================

from datetime import datetime

def deed_restricted_affordable_share(parcel_geog_summary_2020, parcel_geog_summary_2050, modelrun_id, model_run_alias):
    """
    Calculate the share of housing that is deed-restricted affordable in 2020 and 2050

    """
    def calculate_share_dr_units(df_2020, df_2050):
        total_units_2020 = df_2020['residential_units'].sum()
        total_units_2050 = df_2050['residential_units'].sum()
        deed_restricted_units_2020 = df_2020['deed_restricted_units'].sum()
        deed_restricted_units_2050 = df_2050['deed_restricted_units'].sum()
        dr_units_share_2020 = round((deed_restricted_units_2020 / total_units_2020), 2) if total_units_2020 > 0 else 0
        dr_units_share_2050 = round((deed_restricted_units_2050 / total_units_2050), 2) if total_units_2050 > 0 else 0
        
        return (dr_units_share_2020, dr_units_share_2050)
    

    return

    

def new_prod_deed_restricted_affordable_share(parcel_geog_summary_2020, parcel_geog_summary_2050, modelrun_id, model_run_alias):
    """
    Calculate the share of new housing production that is deed-restricted affordable between 2020 and 2050
    """
    def calculate_share_new_dr_units(df_2020, df_2050):
        total_units_2020 = df_2020['residential_units'].sum()
        total_units_2050 = df_2050['residential_units'].sum()
        deed_restricted_units_2020 = df_2020['deed_restricted_units'].sum()
        deed_restricted_units_2050 = df_2050['deed_restricted_units'].sum()

        total_increase = total_units_2050 - total_units_2020
        deed_restricted_increase = deed_restricted_units_2050 - deed_restricted_units_2020

        return round(deed_restricted_increase / total_increase, 2) if total_increase > 0 else 0
    
    
    return

    