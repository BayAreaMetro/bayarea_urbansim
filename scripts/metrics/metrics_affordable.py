# ==============================
# ===== Affordable Metrics =====
# ==============================

import logging, pathlib
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
    area_list = [] # list of areas to summarize
    if rtp == 'RTP2021':
        area_list = ['HRA','EPC','Region']
    elif rtp == 'RTP2025':
        area_list = ['HRA','EPC_18','EPC_22','Region']
    for year in SUMMARY_YEARS:
        for area in area_list:
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
            if (area[:3]=='EPC') and (year==HORIZON_YEAR):
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
    summary_wide_df.insert(0, 'modelrun_alias', f'{HORIZON_YEAR} {modelrun_alias}')

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
    value = None if modelrun_alias in ["No Project"] else 1

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

def housing_cost_share_of_income(
        rtp: str,
        modelrun_alias: str,
        modelrun_id: str,
        modelrun_data: dict,
        metrics_path: pathlib.Path,
        output_path: pathlib.Path,
        append_output: bool  
    ):
    """ Calculates housing cost as a share of household income for income quartiles.

    RTP2021 PBA50: 
    - Task: Share of HH Income Spent on Housing Metric for Draft Blueprint (https://app.asana.com/0/1182463234225195/1172060347607614/f)
    - Memo: Forecasting Income Share Spent on Housing in PBA50.docx (https://mtcdrive.box.com/s/08s6iv7u4mblmbup2zjvrzbsao2pt9iy)
    - Script: https://github.com/BayAreaMetro/regional_forecast/blob/main/housing_income_share_metric/Share_Housing_Costs_Q1-4.R
    - 2019 08 29 Housing Costs Forecast Model.xlsx (https://mtcdrive.box.com/s/c5ru12vio9g286f4bkrj70xr0vgmbud2)
    
    RTP2025 PBA50+:
    - Task: Calculate housing cost as share of household income by income quartile (https://app.asana.com/0/1182463234225195/1206718983365946/f)
    - Initial script: https://github.com/BayAreaMetro/regional_forecast/blob/main/housing_income_share_metric/Share_Housing_Costs_Q1-4_2020.R

    Parameters:
    - rtp (str): RTP2021 or RTP2025.
    - modelrun_alias (str): Alias for the model run, used for labeling output.
    - modelrun_id (str): Identifier for the model run.
    - modelrun_data (dict): year -> {"parcel" -> parcel DataFrame, "county" -> county DataFrame }
    - output_path (str or Path): The directory path to save the output CSV file.
    - append_output (bool): True if appending output; False if writing

    Writes metrics_affordable1_housing_cost_share_of_income.csv to output_path, appending if append_output is True. Columns are:
    - modelrun_alias
    - modelrun_id
    - quartile
    - tenure
    - households
    - share_income
    """
    logging.info("Calculating housing_cost_as_share_of_income")

    SUMMARY_YEARS = sorted(modelrun_data.keys())
    year_initial = SUMMARY_YEARS[0]
    year_horizon = SUMMARY_YEARS[-1]

    # ================================ ASSUMPTIIONS ================================
    # Source: "2019 08 29 Housing Costs Forecast Model.xlsx" (https://mtcdrive.box.com/s/c5ru12vio9g286f4bkrj70xr0vgmbud2)
    # This assumption applies to ALL households (all quartiles, all tenures)
    # TODO: This appears to be based on 2010 data. Update?
    HOUSING_TYPE_SHARE_OF_TOTAL = [
        # year          household_type     household_type_share
        (year_initial,  'deed-restricted',  0.044),
        (year_initial,  'subsidized',       0.027),
        (year_initial,  'price-controlled', 0.058),
        (year_initial,  'market-rate',      0.871), # remainder is market-rate
        # year          household_type     household_type_share
        (year_horizon,  'deed-restricted',  None), # these are just stubs
        (year_horizon,  'subsidized',       None),
        (year_horizon,  'price-controlled', None),
        (year_horizon,  'market-rate',      None),
    ]
    HOUSING_TYPE_SHARE_OF_TOTAL_DF = pd.DataFrame(
        columns=['year','housing_type','housing_type_share'],
        data=HOUSING_TYPE_SHARE_OF_TOTAL)
    assert(HOUSING_TYPE_SHARE_OF_TOTAL_DF.housing_type_share.sum()==1)
    logging.debug(f"HOUSING_TYPE_SHARE_OF_TOTAL_DF:\n{HOUSING_TYPE_SHARE_OF_TOTAL_DF}")

    # Assume these housing types are all rental
    TENURE_SHARE_OF_HOUSING_TYPE = [
        # tenure    housing_type        tenure_share
        ('renter',  'deed-restricted',  1.0),
        ('owner',   'deed-restricted',  0.0),
        # tenure    housing_type        tenure_share
        ('renter',  'subsidized',       1.0),
        ('owner',   'subsidized',       0.0),
        # tenure    housing_type        tenure_share
        ('renter',  'price-controlled',  1.0),
        ('owner',   'price-controlled',  0.0),
    ]
    TENURE_SHARE_OF_HOUSING_TYPE_DF = pd.DataFrame(
        columns=['tenure','housing_type','tenure_share'],
        data=TENURE_SHARE_OF_HOUSING_TYPE
    )
    logging.debug(f"TENURE_SHARE_OF_HOUSING_TYPE_DF:\n{TENURE_SHARE_OF_HOUSING_TYPE_DF}")

    # For each housing_type, this asserts distribution to quartiles
    # Note: select horizon_project varient for horizon_year, non-NoProject scenario
    # TODO: What's the source for this?
    HOUSING_TYPE_SHARE_OF_QUARTILE = [ #                default             horizon_project
        # tenure    quartile        housing_type        housing_type_share  housing_type_share
        ('renter',  'Quartile1',    'deed-restricted',  1.0,                0.668693),
        ('renter',  'Quartile2',    'deed-restricted',  0.0,                0.331307),
        ('renter',  'Quartile3',    'deed-restricted',  0.0,                0.0),
        ('renter',  'Quartile4',    'deed-restricted',  0.0,                0.0),
        #                                               default             horizon_project
        # tenure    quartile        housing_type        housing_type_share  housing_type_share
        ('owner',   'Quartile1',    'deed-restricted',  1.0,                1.0),
        ('owner',   'Quartile2',    'deed-restricted',  0.0,                0.0),
        ('owner',   'Quartile3',    'deed-restricted',  0.0,                0.0),
        ('owner',   'Quartile4',    'deed-restricted',  0.0,                0.0),
        #                                               default             horizon_project
        # tenure    quartile        housing_type        housing_type_share  housing_type_share
        ('renter',  'Quartile1',    'subsidized',       1.0,                1.0),
        ('renter',  'Quartile2',    'subsidized',       0.0,                0.0),
        ('renter',  'Quartile3',    'subsidized',       0.0,                0.0),
        ('renter',  'Quartile4',    'subsidized',       0.0,                0.0),
        #                                               default             horizon_project
        # tenure     quartile        housing_type        housing_type_share  housing_type_share
        ('owner',   'Quartile1',    'subsidized',       1.0,                1.0),
        ('owner',   'Quartile2',    'subsidized',       0.0,                0.0),
        ('owner',   'Quartile3',    'subsidized',       0.0,                0.0),
        ('owner',   'Quartile4',    'subsidized',       0.0,                0.0),
        #                                               default             horizon_project
        # tenure    quartile        housing_type        housing_type_share  housing_type_share
        ('renter',  'Quartile1',    'price-controlled', 0.4,                0.0),
        ('renter',  'Quartile2',    'price-controlled', 0.3,                0.6),
        ('renter',  'Quartile3',    'price-controlled', 0.3,                0.4),
        ('renter',  'Quartile4',    'price-controlled', 0.0,                0.0),
        #                                               default             horizon_project
        # tenure     quartile        housing_type        housing_type_share  housing_type_share
        ('owner',   'Quartile1',    'price-controlled', 0.4,                0.4),
        ('owner',   'Quartile2',    'price-controlled', 0.3,                0.3),
        ('owner',   'Quartile3',    'price-controlled', 0.3,                0.3),
        ('owner',   'Quartile4',    'price-controlled', 0.0,                0.0),
    ]
    HOUSING_TYPE_SHARE_OF_QUARTILE_DF = pd.DataFrame(
        columns=['tenure','quartile','housing_type','default_housing_type_share','horizon_project_housing_type_share'],
        data=HOUSING_TYPE_SHARE_OF_QUARTILE
    )
    logging.debug(f"HOUSING_TYPE_SHARE_OF_QUARTILE_DF:\n{HOUSING_TYPE_SHARE_OF_QUARTILE_DF}")
    HOUSING_TYPES = HOUSING_TYPE_SHARE_OF_QUARTILE_DF.housing_type.unique().tolist()
    HOUSING_TYPES.append('market-rate')
    # logging.debug(f'  {HOUSING_TYPES=}')

    # Share of income spent on housing by quartile and housing_type
    # TODO: What's the source for this?
    SHARE_OF_INCOME_SPENT_ON_HOUSING = [
        # quartile      tenure      deed-res    subsidized
        ('Quartile1',   'renter',   0.29,       0.29),
        ('Quartile2',   'renter',   0.29,       0.29),
        ('Quartile3',   'renter',   0.29,       0.29),
        ('Quartile4',   'renter',   0.29,       0.29),
        # quartile      tenure      deed-res    subsidized
        ('Quartile1',   'owner',    0.29,       0.29),
        ('Quartile2',   'owner',    0.29,       0.29),
        ('Quartile3',   'owner',    0.29,       0.29),
        ('Quartile4',   'owner',    0.29,       0.29),
    ]
    SHARE_OF_INCOME_SPENT_ON_HOUSING_DF = pd.DataFrame(
        columns=['quartile','tenure','share_income deed-restricted','share_income subsidized'],
        data=SHARE_OF_INCOME_SPENT_ON_HOUSING,
    )
    logging.debug(f"SHARE_OF_INCOME_SPENT_ON_HOUSING_DF:\n{SHARE_OF_INCOME_SPENT_ON_HOUSING_DF}")

    # TODO: This is from regional_forecast\housing_income_share_metric\scenario_specific_parameters.csv
    # TODO: What is it based upon?
    AVG_HU_PRICE_RATIO_HORIZON_TO_INITIAL = 0.8427
    # logging.debug(f"{AVG_HU_PRICE_RATIO_HORIZON_TO_INITIAL=}")

    # Assume price-controlled share_income = MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME x (market-rate share_income)
    MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME = 0.857


    if rtp == "RTP2021":
        # root value
        REGIONAL_FORECAST_LEGACY_DIR = "https://raw.githubusercontent.com/BayAreaMetro/regional_forecast/d4eb87f471c40b29b6ca3e3f088952fc5e046b48/housing_income_share_metric/"

        if modelrun_alias == "No Project":
            PUMS_BASEYEAR_HOUSING_COST_FILE = REGIONAL_FORECAST_LEGACY_DIR + "ACS_PUMS_2015_Share_Income_Spent_on_Housing_by_Quartile.csv"
            BAUS_SCENARIO = "s25"
        else:
            # includes UBI
            PUMS_BASEYEAR_HOUSING_COST_FILE = REGIONAL_FORECAST_LEGACY_DIR + "ACS_PUMS_2015_Share_Income_Spent_on_Housing_by_Quartile_UBI.csv"
            BAUS_SCENARIO = "s24"
            # scenarios s26 and s28 appear to have never been explicitly tabulated

        SCENARIO_PARAMS_FILE =  REGIONAL_FORECAST_LEGACY_DIR + "scenario_specific_parameters.csv"
        scenario_params_df = pd.read_csv(SCENARIO_PARAMS_FILE, na_values=['  '])
        scenario_params_df.columns = scenario_params_df.columns.str.strip() # leading whitespace
        # scenario is index and  we only need subset of columns
        scenario_params_df.set_index('scenario', inplace=True)
        scenario_params_df = scenario_params_df[['rdr_units_2050','odr_units_2050','total_rpc_units_2050','total_opc_units_2050']]
        scenario_params_df = scenario_params_df.replace({' ':None}).astype('float')
        logging.info(f"  Read {len(scenario_params_df)} lines from {SCENARIO_PARAMS_FILE}")
        logging.debug(f"\n{scenario_params_df}")
        logging.debug(f"\n{scenario_params_df.dtypes}")

        scenario_params_df = scenario_params_df.loc[BAUS_SCENARIO] # keep only this scenario
        # format this more usefully: convert single row to column
        scenario_params_df = pd.DataFrame(scenario_params_df.transpose())
        scenario_params_df.columns = ['BAUS_households'] # rename that column
        scenario_params_df.index = scenario_params_df.index.str.replace('total_','') #index is original column names
        scenario_params_df.index = scenario_params_df.index.str.replace('_units_2050','')
        scenario_params_df['year'] = 2050
        scenario_params_df['tenure'] = scenario_params_df.index.str[0]
        scenario_params_df['housing_type'] = scenario_params_df.index.str[1:]
        # recode
        scenario_params_df.replace({
            'tenure':{'o':'owner','r':'renter'},
            'housing_type':{'dr':'deed-restricted', 'pc':'price-controlled'}
        }, inplace=True)
        scenario_params_df.reset_index(drop=True, inplace=True)
        logging.debug(f"\n{scenario_params_df}")
        logging.debug(f"\n{scenario_params_df.dtypes}")

    elif rtp == "RTP2025":
        # TODO: why is this 2019 and as recent as possible (2021)?
        PUMS_BASEYEAR_HOUSING_COST_FILE = metrics_path / "metrics_input_files" / "ACS_PUMS_2019_Share_Income_Spent_on_Housing_by_Quartile.csv"

        if modelrun_alias == "No Project":
            BAUS_SCENARIO = "RTP2025_NP"
        elif modelrun_alias=="Draft Blueprint:
            BAUS_SCENARIO = "RTP2025_DBP"
        else:
            BAUS_SCENARIO = "RTP2025_FBP"

        # TODO: This is duplicate code because I think it should be removed
        # TODO: Deed restricted unit counts should come from deed_restricted_affordable_share() results
        # TODO: Price controlled unit count appears to be fixed at 144892 for all scenarios; what is this based upon?
        SCENARIO_PARAMS_FILE =  metrics_path / "metrics_input_files" / "scenario_specific_parameters.csv"
        scenario_params_df = pd.read_csv(SCENARIO_PARAMS_FILE, na_values=['  '])
        scenario_params_df.columns = scenario_params_df.columns.str.strip() # leading whitespace
        # scenario is index and  we only need subset of columns
        scenario_params_df.set_index('scenario', inplace=True)
        scenario_params_df = scenario_params_df[['rdr_units_2050','odr_units_2050','total_rpc_units_2050','total_opc_units_2050']]
        scenario_params_df = scenario_params_df.replace({' ':None}).astype('float')
        # logging.info(f"  Read {len(scenario_params_df)=:,} lines from {SCENARIO_PARAMS_FILE}")
        logging.debug(f"\n{scenario_params_df}")
        logging.debug(f"\n{scenario_params_df.dtypes}")

        scenario_params_df = scenario_params_df.loc[BAUS_SCENARIO] # keep only this scenario
        # format this more usefully: convert single row to column
        scenario_params_df = pd.DataFrame(scenario_params_df.transpose())
        scenario_params_df.columns = ['BAUS_households'] # rename that column
        scenario_params_df.index = scenario_params_df.index.str.replace('total_','') #index is original column names
        scenario_params_df.index = scenario_params_df.index.str.replace('_units_2050','')
        scenario_params_df['year'] = 2050
        scenario_params_df['tenure'] = scenario_params_df.index.str[0]
        scenario_params_df['housing_type'] = scenario_params_df.index.str[1:]
        # recode
        scenario_params_df.replace({
            'tenure':{'o':'owner','r':'renter'},
            'housing_type':{'dr':'deed-restricted', 'pc':'price-controlled'}
        }, inplace=True)
        scenario_params_df.reset_index(drop=True, inplace=True)
        logging.debug(f"\n{scenario_params_df}")
        logging.debug(f"\n{scenario_params_df.dtypes}")

    pums_baseyear_housing_cost_df = pd.read_csv(PUMS_BASEYEAR_HOUSING_COST_FILE)
    pums_baseyear_housing_cost_df['tenure'] = pums_baseyear_housing_cost_df.tenure.str.lower()
    pums_baseyear_housing_cost_df['tenure'] = pums_baseyear_housing_cost_df['tenure'].replace({'total':'all_tenures'})
    logging.info(f"  Read {len(pums_baseyear_housing_cost_df):,} lines from {PUMS_BASEYEAR_HOUSING_COST_FILE}")
    logging.debug(f"\n{pums_baseyear_housing_cost_df}")
    # columns are: quartile, tenure, aggregate_income, aggregate_rent, aggregate_owncosts, aggregate_costs, 
    #              households, share_income, short_name

    # calculate the share of households that are renter, by quartile
    pums_renter_share_df = pums_baseyear_housing_cost_df[['quartile','tenure','households']].pivot(index='quartile', columns='tenure', values='households')
    pums_renter_share_df['PUMS_renter_share'] = pums_renter_share_df.renter / pums_renter_share_df.all_tenures
    pums_renter_share_df.drop(columns=['owner','renter','all_tenures'], inplace=True)
    logging.debug(f"pums_renter_share_df:\n{pums_renter_share_df}")

    # Create county sumary dataframe for initial and horizon year
    # with columns: quartile, all households {year_initial}, all households {year_horizon}
    household_count_df = pd.DataFrame({
        f'all households {year_initial}':modelrun_data[year_initial]['county'][['hhincq1','hhincq2','hhincq3','hhincq4']].sum(),
        f'all households {year_horizon}':modelrun_data[year_horizon]['county'][['hhincq1','hhincq2','hhincq3','hhincq4']].sum(),
    }).reset_index(drop=False, names='quartile')
    household_count_df['quartile'] = household_count_df.quartile.str.replace('hhincq','Quartile')

    if rtp=="RTP2021" and modelrun_alias != "No Project":
        # The original script asserted these
        # https://github.com/BayAreaMetro/regional_forecast/blob/cc4eade1bcdb9ae92a08556c7389257a54aefcaa/housing_income_share_metric/Share_Housing_Costs_Q1-4.R#L148
        household_count_df.set_index('quartile', inplace=True)
        household_count_df.at['Quartile1', f'all households {year_horizon}'] = 1009965
        household_count_df.at['Quartile2', f'all households {year_horizon}'] =  920534
        household_count_df.reset_index(drop=False, inplace=True)

    logging.debug(f"household_count_df:\n{household_count_df}")
    # move year to column
    household_count_df = pd.wide_to_long(
        household_count_df, stubnames='all households',
        i='quartile', j='year', sep=' ', suffix='\\d+').reset_index(drop=False)
    logging.debug(f"household_count_df after wide_to_long():\n{household_count_df}")

    ######### Step 1: assume pums_renter share and split household into owners vs renters
    household_count_df = pd.merge(
        left     = household_count_df,
        right    = pums_renter_share_df,
        how      = 'inner',
        validate = 'many_to_one',
        on       = 'quartile'
    )
    logging.debug(f"household_count_df after merge with pums_renter_share_df:\n{household_count_df}")
    household_count_df['renter households'] = household_count_df['all households']*household_count_df.PUMS_renter_share

    # round to integer households
    household_count_df['renter households'] = household_count_df['renter households'].round().astype('int')

    # remaining are owners
    household_count_df['owner households'] = household_count_df['all households'] - household_count_df['renter households']
    # drop - we're done with this
    household_count_df.drop(columns=['all households','PUMS_renter_share'], inplace=True)

    # move tenure to its own column
    # columns are now: quartile, year, tenure, households
    household_count_df = pd.melt(
        household_count_df.reset_index(drop=False), id_vars=['year','quartile'],
        value_vars=['renter households','owner households'],
        var_name='tenure', value_name='households')
    household_count_df['tenure'] = household_count_df['tenure'].str.replace(' households','')
    logging.debug(f"household_count_df:\n{household_count_df}")
    
    # summarize by year only
    household_year_totals_df = household_count_df.groupby(by=['year']).agg({'households':'sum'})
    logging.debug(f"household_year_totals_df:\n{household_year_totals_df}")
    ######### Step 2: Determine overall levels of initial_year housing_type from HOUSING_TYPE_SHARE_OF_TOTAL_DF
    housingtype_df = pd.merge(
        left  = household_year_totals_df.reset_index(drop=False),
        right = HOUSING_TYPE_SHARE_OF_TOTAL_DF, 
        on    = ['year'],
        how   = 'left'
    )
    housingtype_df['households'] = housingtype_df.households*housingtype_df.housing_type_share
    housingtype_df = housingtype_df[['year','housing_type','households']]
    logging.debug(f"housingtype_df after join with HOUSING_TYPE_SHARE_OF_TOTAL_DF:\n{housingtype_df}")

    ######### Step 3a: For year_initial, assume tenure shares of housing types from TENURE_SHARE_OF_HOUSING_TYPE_DF
    housingtype_df = pd.merge(
        left  = housingtype_df,
        right = TENURE_SHARE_OF_HOUSING_TYPE_DF,
        how   = 'inner',
        on    = ['housing_type']
    )
    housingtype_df['households'] = housingtype_df.households*housingtype_df.tenure_share
    housingtype_df.drop(columns=['tenure_share'], inplace=True)
    logging.debug(f"housingtype_df after tenure_share is applied:\n{housingtype_df}")

    ######### Step 3b: For year_horizon, use BAUS households for deed-restricted and price-controlled
    housingtype_df = pd.merge(
        left    = housingtype_df,
        right   = scenario_params_df,
        how     = 'left',
        on      = ['year','tenure','housing_type']
    )
    housingtype_df.loc[ pd.notnull(housingtype_df.BAUS_households), 'households'] = housingtype_df.BAUS_households
    housingtype_df.drop(columns=['BAUS_households'],inplace=True)
    housingtype_df.set_index(keys=['year','tenure','housing_type'], inplace=True)
    logging.debug(f"housingtype_df after BAUS deed-restricted/price-controlled is included:\n{housingtype_df}")

    # for year_horizon, assume subsidized proportion for that tenure persists
    households_initial_year = int(household_year_totals_df.at[year_initial, 'households'])
    households_horizon_year = int(household_year_totals_df.at[year_horizon, 'households'])
    logging.debug(f'  {households_initial_year:,}  {households_horizon_year:,}')
    for tenure in ['renter','owner']:
        households_subsidized_initial_year = housingtype_df.at[(year_initial, tenure, 'subsidized'), 'households']
        logging.debug(f'  for {tenure}, applying proportion {households_subsidized_initial_year/households_initial_year}')
        
        housingtype_df.at[ (year_horizon, tenure, 'subsidized'), 'households'] = \
            households_horizon_year*households_subsidized_initial_year/households_initial_year
    housingtype_df.reset_index(drop=False, inplace=True)
    logging.debug(f"housingtype_df after scenario_params_df is applied:\n{housingtype_df}")

    ######### Step 4: Apply housing_type distribution by quartile from HOUSING_TYPE_SHARE_OF_QUARTILE_DF
    housingtype_df = pd.merge(
        left  = housingtype_df,
        right = HOUSING_TYPE_SHARE_OF_QUARTILE_DF,
        how   = 'left',
        on    = ['tenure','housing_type'],
    )
    # There are two housing_type_share columns: default_housing_type_share and horizon_project_housing_type_share
    # Select the appropriate one
    logging.debug(f"housingtype_df with HOUSING_TYPE_SHARE_OF_QUARTILE_DF:\n{housingtype_df}")
    housingtype_df['housing_type_share'] = housingtype_df.default_housing_type_share
    # use this column for future project (e.g. non no-project)
    if modelrun_alias != "No Project":
        housingtype_df.loc[ housingtype_df.year == year_horizon, 
                           'housing_type_share'] = housingtype_df.horizon_project_housing_type_share

    housingtype_df['households'] = housingtype_df.housing_type_share * housingtype_df.households
    housingtype_df.drop(columns=['housing_type_share'], inplace=False)
    housingtype_df = housingtype_df[['year','tenure','quartile','housing_type','households']]
    # round
    housingtype_df['households'] = housingtype_df['households'].round().astype('int')
    logging.debug(f"housingtype_df with quartiles:\n{housingtype_df}")

    # move housing_type to column
    housingtype_df = housingtype_df.pivot(index=['year','tenure','quartile'], columns='housing_type', values='households')
    housingtype_df.columns.name = None
    housingtype_df.reset_index(drop=False, inplace=True)
    housingtype_df.rename(columns={f'{housing_type}':f'households {housing_type}' for housing_type in HOUSING_TYPES}, inplace=True)
    logging.debug(f"housingtype_df after pivot:\n{housingtype_df}")

    ######### Step 5: Assume remaining households are in market-rate units
    # join back to household_count_df
    household_count_df = pd.merge(
        left  = household_count_df,
        right = housingtype_df,
        how   = 'left',
        on    = ['year','tenure','quartile']
    )
    household_count_df['households'] = household_count_df['households'].astype('int')

    # assume all market-rate, and subtract out deed-restricted, price-controlled and subsidized
    household_count_df['households market-rate'] = household_count_df['households']
    for housing_type in ['deed-restricted','price-controlled','subsidized']:
        household_count_df.loc[ pd.notnull(household_count_df[f'households {housing_type}']), 'households market-rate'] = \
            household_count_df['households market-rate'] - household_count_df[f'households {housing_type}']
    logging.debug(f"household_count_df:\n{household_count_df}")

    # Debug: display it in a similar format to R script debug files, full_YYYY.csv
    debug_hhcounts_df = household_count_df.copy()
    debug_hhcounts_df['year_inc_ten'] = debug_hhcounts_df.year.astype('str') + "_" + debug_hhcounts_df.quartile + "_" + debug_hhcounts_df.tenure
    debug_hhcounts_df.drop(columns=['year','quartile','tenure'], inplace=True)
    debug_hhcounts_df.set_index(['year_inc_ten'], inplace=True)
    debug_hhcounts_df.index = debug_hhcounts_df.index.str.replace("renter","_renter") # to sort renter before owner
    debug_hhcounts_df.sort_index(inplace=True)
    # sort columns
    debug_hhcounts_df = debug_hhcounts_df[['households deed-restricted','households subsidized','households price-controlled','households market-rate','households']]
    debug_hhcounts_df = debug_hhcounts_df.transpose()
    logging.debug(f"debug_hhcounts_df:\n{debug_hhcounts_df}")

    #########
    # start with these assumptions
    initial_share_spent_df = SHARE_OF_INCOME_SPENT_ON_HOUSING_DF.copy()
    # and pull totals from pums
    initial_share_spent_df = pd.merge(
        left    = initial_share_spent_df,
        right   = pums_baseyear_housing_cost_df[['quartile','tenure','share_income']],
        how     = 'left',
        on      = ['quartile','tenure'],
    )
    logging.debug(f"initial_share_spent_df:\n{initial_share_spent_df}")

    # put housing counts together with year_initial shares
    initial_summary_df = pd.merge(
        left    = household_count_df.loc[household_count_df.year == year_initial],
        right   = initial_share_spent_df,
        how     = 'left',
        on      = ['quartile','tenure']
    )
    initial_summary_df.sort_values(by=['quartile','tenure'], ascending=[True,False],inplace=True)
    logging.debug(f"initial_summary_df:\n{initial_summary_df}")

    # TODO: Note the following logic is only happening for year_initial
    # TODO: I'm not sure why the same logic wouldn't be used for year_horizon...
    #
    # Starting with equation to get weighted average of housing unit types: 'share' is share of income paid 
    # and households is count of units of that type (by income quartile and tenure):
    #
    # (dr_count*dr_share*)+(su_count*su_share)+(pc_count*pc_share)+(ma_count*ma_share)=(total_count*total_share)
    #
    # Assuming pc_share = ma_share*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME
    # 
    # (dr_count*dr_share*)+(su_count*su_share)+(pc_count*ma_share*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME)+(ma_count*ma_share)=(total_count*total_share)
    #
    # (dr_count*dr_share)+(su_count*su_share)+(ma_share*(pc_count*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME + ma_count))==(total_count*total_share)
    #
    # (ma_share*(pc_count*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME + ma_count)) =
    #   (total_count*total_share) - (dr_count*dr_share) - (su_count*su_share)
    #
    # ma_share = [(total_count*total_share) - (dr_count*dr_share) - (su_count*su_share)] / 
    #             (pc_count*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME + ma_count)

    initial_summary_df['share_income market-rate'] = \
        ((initial_summary_df['households'                 ]*initial_summary_df['share_income']) - \
         (initial_summary_df['households deed-restricted' ]*initial_summary_df['share_income deed-restricted']) - \
         (initial_summary_df['households subsidized'      ]*initial_summary_df['share_income subsidized'])) / \
        ((initial_summary_df['households price-controlled']*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME) + initial_summary_df['households market-rate'])
    
    # Assuming pc_share = ma_share*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME
    initial_summary_df['share_income price-controlled'] = initial_summary_df['share_income market-rate']*MARKET_RATE_TO_PRICE_CONTROL_SHARE_INCOME
    
    # copy year_initial shares to year_horizon
    horizon_share_spent_df = initial_summary_df[['quartile','tenure','share_income'] +
                                        [f'share_income {housing_type}' for housing_type in HOUSING_TYPES]].copy()
    horizon_share_spent_df['year'] = year_horizon

    # for horizon year, factor up price-controlled share for renters
    horizon_share_spent_df.loc[ (horizon_share_spent_df.tenure == 'renter'), 'share_income price-controlled'] = \
        horizon_share_spent_df['share_income price-controlled'] * AVG_HU_PRICE_RATIO_HORIZON_TO_INITIAL
    # for horizon year, factor up market-rate share for everyone
    horizon_share_spent_df['share_income market-rate'] = \
        horizon_share_spent_df['share_income market-rate'] * AVG_HU_PRICE_RATIO_HORIZON_TO_INITIAL
    logging.debug(f"horizon_share_spent_df:\n{horizon_share_spent_df}")

    # put year_horizon shares with year_horizon household counts
    horizon_summary_df = pd.merge(
        left    = household_count_df.loc[household_count_df.year == year_horizon],
        right   = horizon_share_spent_df,
        how     = 'left',
        on      = ['year','quartile','tenure']
    )
    horizon_summary_df.sort_values(by=['quartile','tenure'], ascending=[True,False],inplace=True)
    logging.debug(f"horizon_summary_df:\n{horizon_summary_df}")

    # Debug: display it in a similar format to R script debug files, hh_income_matrix_YYYY_filled.csv
    debug_share_df = horizon_share_spent_df.copy()
    debug_share_df['year_inc_ten'] = debug_share_df.year.astype('str') + "_" + debug_share_df.quartile + "_" + debug_share_df.tenure
    debug_share_df.drop(columns=['year','quartile','tenure'], inplace=True)
    debug_share_df.set_index(['year_inc_ten'], inplace=True)
    debug_share_df.index = debug_share_df.index.str.replace("renter","_renter") # to sort renter before owner
    debug_share_df.sort_index(inplace=True)
    # sort columns
    debug_share_df = debug_share_df.transpose()
    logging.debug(f"debug_share_df:\n{debug_share_df}")


    # move housing_type to one column
    horizon_summary_df = pd.wide_to_long(
        horizon_summary_df.drop(columns=['households','share_income']),
        stubnames = ['households','share_income'],
        i         = ['year','quartile','tenure'],
        j         = 'housing_type',
        sep       = ' ',
        suffix    = '\D+').reset_index(drop=False)

    # we want share_income weighted by households -- so multiply by households
    horizon_summary_df['households x share_income'] = horizon_summary_df.households * horizon_summary_df.share_income
    logging.debug(f"horizon_summary_df:\n{horizon_summary_df}")

    # summarize to year/quartile/tenure
    horizon_year_quartile_tenure_df = horizon_summary_df.groupby(by=['year','quartile','tenure']).agg({
        'households x share_income': 'sum',
        'households':'sum'
    }).reset_index(drop=False)

    # summarize to year/quartile
    horizon_year_quartile_df = horizon_summary_df.groupby(by=['year','quartile']).agg({
        'households x share_income': 'sum',
        'households':'sum'
    }).reset_index(drop=False)
    horizon_year_quartile_df['tenure'] = 'all_tenures'

    # summarize to year
    horizon_year_df = horizon_summary_df.groupby(by=['year']).agg({
        'households x share_income': 'sum',
        'households':'sum'
    }).reset_index(drop=False)
    horizon_year_df['tenure']   = 'all_tenures'
    horizon_year_df['quartile'] = 'all_incomes'


    # put it together with year_initial summary, for which we'll use pums directly (NOT initial_summary_df)
    # this already includes disaggregate and aggregate tenure
    pums_summary_df = pums_baseyear_housing_cost_df[['quartile','tenure','households','share_income']].copy()
    pums_summary_df['year'] = year_initial

    # but we do need to do all quartiles/all tenures aggregation
    pums_summary_df['households x share_income'] = pums_summary_df.households * pums_summary_df.share_income
    # aggregate from rows with tenure defined
    pums_summary_year_df = pums_summary_df.loc[pums_summary_df.tenure != 'all_tenures'].groupby(by=['year']).agg({
        'households x share_income': 'sum',
        'households':'sum'
    }).reset_index(drop=False)
    pums_summary_year_df['tenure']   = 'all_tenures'
    pums_summary_year_df['quartile'] = 'all_incomes'

    # put everything together
    result_df = pd.concat([
        pums_summary_df, # this has both tenure values and tenure aggregated
        pums_summary_year_df,
        horizon_year_quartile_tenure_df,
        horizon_year_quartile_df,
        horizon_year_df
    ]).reset_index(drop=True)
    logging.debug(f"result_df:\n{result_df}")

    # calculate this for those that need it
    result_df.loc[ pd.isnull(result_df.share_income), 'share_income'] = \
        result_df['households x share_income'] / result_df['households']
    
    # package it up
    result_df['modelrun_id'   ] = modelrun_id
    result_df['modelrun_alias'] = result_df.year.astype('str') + ' ' + modelrun_alias
    result_df = result_df[['modelrun_id','modelrun_alias','year','quartile','tenure','households','share_income']]
    logging.debug(f"result_df:\n{result_df}")

    # write it
    filename = "metrics_affordable1_housing_cost_share_of_income.csv"
    filepath = output_path / filename
    result_df.to_csv(filepath, mode='a' if append_output else 'w', header=False if append_output else True, index=False)
    logging.info("{} {:,} lines to {}".format("Appended" if append_output else "Wrote", len(result_df), filepath))
    
