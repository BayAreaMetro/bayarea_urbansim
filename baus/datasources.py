from __future__ import print_function

import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import orca
from baus.utils import nearest_neighbor
import yaml


#####################
# TABLES AND INJECTABLES
#####################


# define new settings files- these have been subdivided from the
# general settings file
# this is similar to the code for settings in urbansim_defaults
@orca.injectable('run_setup', cache=True)
def run_setup():
    with open("../run_setup.yaml") as f:
        return yaml.load(f)


@orca.injectable('run_name', cache=True)
def run_name(run_setup):
    return os.path.join(run_setup["run_name"])


@orca.injectable('inputs_dir', cache=True)
def inputs_dir(run_setup):
    return run_setup['inputs_dir']


@orca.injectable('outputs_dir', cache=True)
def outputs_dir(run_setup):
    return os.path.join(run_setup['outputs_dir'], run_setup["run_name"])


@orca.injectable('viz_dir', cache=True)
def viz_dir(run_setup):
    return os.path.join(run_setup['viz_dir'])


@orca.injectable('paths', cache=True)
def paths():
    with open(os.path.join(misc.configs_dir(), "paths.yaml")) as f:
        return yaml.load(f)


@orca.injectable('accessibility_settings', cache=True)
def accessibility_settings():
    with open(os.path.join(misc.configs_dir(), "accessibility/accessibility_settings.yaml")) as f:
        return yaml.load(f)


@orca.injectable('transition_relocation_settings', cache=True)
def transition_relocation_settings():
    with open(os.path.join(misc.configs_dir(), "transition_relocation/transition_relocation_settings.yaml")) as f:
        return yaml.load(f)


@orca.injectable('profit_adjustment_strategies', cache=True)
def profit_adjustment_strategies():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/profit_adjustment_strategies.yaml")) as f:
        return yaml.load(f)


@orca.injectable('account_strategies', cache=True)
def account_strategies():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/account_strategies.yaml")) as f:
        return yaml.load(f)


@orca.injectable('development_caps', cache=True)
def development_caps():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/existing_policy/development_caps.yaml")) as f:
        return yaml.load(f)


@orca.injectable('development_caps_asserted', cache=True)
def development_caps_asserted():
    with open(os.path.join(misc.configs_dir(), "adjusters/development_caps_asserted.yaml")) as f:
        return yaml.load(f)


@orca.injectable('zoning_adjusters', cache=True)
def zoning_adjusters():
    with open(os.path.join(misc.configs_dir(), "adjusters/zoning_adjusters.yaml")) as f:
        return yaml.load(f)


@orca.injectable('development_caps_strategy', cache=True)
def development_caps_strategy():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/development_caps_strategy.yaml")) as f:
        return yaml.load(f)


@orca.injectable('inclusionary', cache=True)
def inclusionary():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/existing_policy/inclusionary.yaml")) as f:
        return yaml.load(f)


@orca.injectable('inclusionary_strategy', cache=True)
def inclusionary_strategy():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/inclusionary_strategy.yaml")) as f:
        return yaml.load(f)


@orca.injectable('preservation', cache=True)
def preservation():
    with open(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/preservation.yaml")) as f:
        return yaml.load(f)


@orca.injectable('cost_shifters', cache=True)
def cost_shifters():
    with open(os.path.join(misc.configs_dir(), "adjusters/cost_shifters.yaml")) as f:
        return yaml.load(f)


@orca.injectable('developer_settings', cache=True)
def developer_settings():
    with open(os.path.join(misc.configs_dir(), "developer/developer_settings.yaml")) as f:
        return yaml.load(f)


@orca.injectable('price_settings', cache=True)
def price_settings():
    with open(os.path.join(misc.configs_dir(), "hedonics/price_settings.yaml")) as f:
        return yaml.load(f)


# now that there are new settings files, override the locations of certain
# settings already defined in urbansim_defaults

# this just adds some of the BAUS settings to a master "settings", since the urbansim code looks for them there
@orca.injectable("settings")
def settings(developer_settings, transition_relocation_settings):
    settings = developer_settings.copy()
    settings.update(transition_relocation_settings)    
    return settings


@orca.injectable("building_type_map")
def building_type_map(developer_settings):
    return developer_settings["building_type_map"]


@orca.injectable('year')
def year():
    try:
        return orca.get_injectable("iter_var")
    except Exception as e:
        pass
    # if we're not running simulation, return base year
    return 2020


@orca.injectable()
def initial_year():
    return 2010


@orca.injectable()
def initial_summary_year():
    return 2015


@orca.injectable()
def interim_summary_year():
    return 2035


@orca.injectable()
def final_year():
    return 2050


@orca.injectable(cache=True)
def store():
    return pd.HDFStore(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/parcels_buildings_agents/core_datasets.h5"))


@orca.injectable(cache=True)
def limits_settings(development_caps, run_setup):
    # for limits, we inherit from the default settings, and update these with the policy settings, if applicable
    # limits set the annual maximum number of job spaces or residential units that may be built in a geography

    d = development_caps['development_limits']["default"]
    if run_setup["asserted_development_caps"]:
        development_caps_asserted = orca.get_injectable("development_caps_asserted")
        d2 = development_caps_asserted['development_limits']["default"]
        d.update(d2)

    if run_setup['run_job_cap_strategy']:
        print("Applying job caps")
        development_caps_strategy = orca.get_injectable("development_caps_strategy")
        d_jc = development_caps_strategy['development_limits']["job_cap_strategy"]

        for key, value in d_jc.items():
            d.setdefault(key, {})
            d[key].update(value)

        return d

    print("Using default limits")
    return d


@orca.injectable(cache=True)
def inclusionary_housing_settings(inclusionary, run_setup):
    # for inclusionary housing, there is no inheritance from the default inclusionary settings
    # this means existing inclusionary levels in the base year don't apply in the policy application...

    if run_setup["run_inclusionary_strategy"]:
        inclusionary_strategy = orca.get_injectable("inclusionary_strategy")
        s = inclusionary_strategy['inclusionary_housing_settings']["inclusionary_strategy"]
    else:
        s = inclusionary['inclusionary_housing_settings']["default"]

    d = {}
    for item in s:
        # turn list of inclusionary rates and the geographies they apply to to a map of geography names to inclusionary rates
        print("Setting inclusionary rates for %d %s to %.2f" % (len(item["values"]), item["type"], item["amount"]))
        for geog in item["values"]:
            d[geog] = item["amount"]

    return d


# we only need this because we've written our own interim output code
# and don't need urbansim to attempt to write another set
@orca.injectable("summary", cache=True)
def summary():
    return np.nan


@orca.injectable(cache=True)
def building_sqft_per_job(developer_settings):
    return developer_settings['building_sqft_per_job']


# key locations in the Bay Area for use as attractions in the models
@orca.table(cache=True)
def landmarks():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 'accessibility/pandana/landmarks.csv'),
                       index_col="name")


@orca.table(cache=True)
def base_year_summary_taz(mapping):
    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/baseyear_taz_summaries.csv"), 
                     dtype={'taz1454': np.int64}, index_col="zone_id")
    cmap = mapping["county_id_tm_map"]
    df['COUNTY_NAME'] = df.COUNTY.map(cmap)
    return df


# non-residential rent data
@orca.table(cache=True)
def costar(parcels):
    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 'basis_inputs/parcels_buildings_agents/2015_08_29_costar.csv'))

    df["PropertyType"] = df.PropertyType.replace("General Retail", "Retail")
    df = df[df.PropertyType.isin(["Office", "Retail", "Industrial"])]

    df["costar_rent"] = df["Average Weighted Rent"].astype('float')
    df["year_built"] = df["Year Built"].fillna(1980)

    df = df.dropna(subset=["costar_rent", "Latitude", "Longitude"])

    # now assign parcel id
    df["parcel_id"] = nearest_neighbor(parcels.to_frame(['x', 'y']).dropna(subset=['x', 'y']), df[['Longitude', 'Latitude']])

    return df


@orca.table(cache=True)
def zoning_existing():
    return os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/existing_policy/boc.csv")


@orca.table(cache=True)
def proportional_retail_jobs_forecast():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/proportional_retail_jobs_forecast.csv"), 
                       index_col="juris")


@orca.table(cache=True)
def proportional_gov_ed_jobs_forecast():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/proportional_gov_ed_jobs_forecast.csv"), 
                       index_col="Taz")


@orca.table(cache=True)
def travel_model_zones():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/crosswalks/travel_model_zones.csv"))


@orca.table(cache=True)
def tm2_occupation_shares():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm2_occupation_shares.csv"))


@orca.table(cache=True)
def tm2_taz2_forecast_inputs(tm1_tm2_regional_demographic_forecast):
    t2fi = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm2_taz2_forecast_inputs.csv"), 
                       dtype={'TAZ': np.int64}, index_col='TAZ').replace('#DIV/0!', np.nan)

    rdf = tm1_tm2_regional_demographic_forecast.to_frame()
    # apply regional share of hh by size to MAZs with no households in 2010
    t2fi.loc[t2fi.shrw0_2010.isnull(), 'shrw0_2010'] = rdf.loc[rdf.year == 2010, 'shrw0'].values[0]
    t2fi.loc[t2fi.shrw1_2010.isnull(), 'shrw1_2010'] = rdf.loc[rdf.year == 2010, 'shrw1'].values[0]
    t2fi.loc[t2fi.shrw2_2010.isnull(), 'shrw2_2010'] = rdf.loc[rdf.year == 2010, 'shrw2'].values[0]
    t2fi.loc[t2fi.shrw3_2010.isnull(), 'shrw3_2010'] = rdf.loc[rdf.year == 2010, 'shrw3'].values[0]

    # apply regional share of persons by age category
    t2fi.loc[t2fi.shra1_2010.isnull(), 'shra1_2010'] = rdf.loc[rdf.year == 2010, 'shra1'].values[0]
    t2fi.loc[t2fi.shra2_2010.isnull(), 'shra2_2010'] = rdf.loc[rdf.year == 2010, 'shra2'].values[0]
    t2fi.loc[t2fi.shra3_2010.isnull(), 'shra3_2010'] = rdf.loc[rdf.year == 2010, 'shra3'].values[0]
    t2fi.loc[t2fi.shra4_2010.isnull(), 'shra4_2010'] = rdf.loc[rdf.year == 2010, 'shra4'].values[0]

    # apply regional share of hh by presence of children
    t2fi.loc[t2fi.shrn_2010.isnull(), 'shrn_2010'] = rdf.loc[rdf.year == 2010, 'shrn'].values[0]
    t2fi.loc[t2fi.shry_2010.isnull(), 'shry_2010'] = rdf.loc[rdf.year == 2010, 'shry'].values[0]

    t2fi[['shrw0_2010', 'shrw1_2010', 'shrw2_2010', 'shrw3_2010', 'shra1_2010', 'shra2_2010', 'shra3_2010', 'shra4_2010', 'shrn_2010',
          'shry_2010']] = t2fi[['shrw0_2010', 'shrw1_2010', 'shrw2_2010', 'shrw3_2010', 'shra1_2010', 'shra2_2010', 'shra3_2010', 
                                'shra4_2010', 'shrn_2010', 'shry_2010']].astype('float')

    return t2fi


@orca.table(cache=True)
def tm2_emp27_employment_shares():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm2_emp27_employment_shares.csv"))


@orca.table(cache=True)
def tm1_tm2_maz_forecast_inputs(tm1_tm2_regional_demographic_forecast):
    rdf = tm1_tm2_regional_demographic_forecast.to_frame()
    mfi = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm1_tm2_maz_forecast_inputs.csv"),
                      dtype={'MAZ': np.int64}, index_col='MAZ').replace('#DIV/0!', np.nan)

    # apply regional share of hh by size to MAZs with no households in 2010
    mfi.loc[mfi.shrs1_2010.isnull(),
            'shrs1_2010'] = rdf.loc[rdf.year == 2010, 'shrs1'].values[0]
    mfi.loc[mfi.shrs2_2010.isnull(),
            'shrs2_2010'] = rdf.loc[rdf.year == 2010, 'shrs2'].values[0]
    mfi.loc[mfi.shrs3_2010.isnull(),
            'shrs3_2010'] = rdf.loc[rdf.year == 2010, 'shrs3'].values[0]
    # the fourth category here is missing the 'r' in the csv
    mfi.loc[mfi.shs4_2010.isnull(),
            'shs4_2010'] = rdf.loc[rdf.year == 2010, 'shrs4'].values[0]
    mfi[['shrs1_2010', 'shrs2_2010', 'shrs3_2010',
         'shs4_2010']] = mfi[['shrs1_2010', 'shrs2_2010',
                              'shrs3_2010', 'shs4_2010']].astype('float')
    return mfi


@orca.table(cache=True)
def zoning_strategy(growth_geographies, developer_settings):

    strategy_zoning = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 'plan_strategies/zoning_mods.csv'))

    for k in developer_settings["building_type_map"].keys():
        strategy_zoning[k] = np.nan

    def add_drop_helper(col, val):
        for ind, item in strategy_zoning[col].items():
            if not isinstance(item, str):
                continue
            for btype in item.split():
                strategy_zoning.loc[ind, btype] = val

    add_drop_helper("add_bldg", 1)
    add_drop_helper("drop_bldg", 0)
                                       
    join_col = 'zoningmodcat'
    print('join_col of zoningmods is {}'.format(join_col))

    return pd.merge(growth_geographies.to_frame().reset_index(), strategy_zoning, on=join_col, how='left').set_index('parcel_id')


@orca.table(cache=True)
def parcels(store):
    df = store['parcels']


@orca.table(cache=True)
def parcels_zoning_calculations(parcels):
    return pd.DataFrame(index=parcels.index)


@orca.table(cache=True)
def growth_geographies():
    return os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/crosswalks/growth_geographies.csv")
    

@orca.table(cache=False)
def mandatory_accessibility(year, run_setup):

    if year in run_setup['logsum_period1']:
        df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                         "accessibility/travel_model/mandatoryAccessibilities_{}.csv").format(run_setup['logsum_year1']))
    elif year in run_setup['logsum_period2']:
        df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                         "accessibility/travel_model/mandatoryAccessibilities_{}.csv").format(run_setup['logsum_year2']))

    df.loc[df.subzone == 0, 'subzone'] = 'c'  # no walk
    df.loc[df.subzone == 1, 'subzone'] = 'a'  # short walk
    df.loc[df.subzone == 2, 'subzone'] = 'b'  # long walk
    df['taz_sub'] = df.taz.astype('str') + df.subzone

    return df.set_index('taz_sub')


@orca.table(cache=False)
def non_mandatory_accessibility(year, run_setup):

    if year in run_setup['logsum_period1']:
        df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                         "accessibility/travel_model/nonMandatoryAccessibilities_{}.csv").format(run_setup['logsum_year1']))
    elif year in run_setup['logsum_period2']:
        df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                         "accessibility/travel_model/nonmandatoryAccessibilities_{}.csv").format(run_setup['logsum_year2']))

    df.loc[df.subzone == 0, 'subzone'] = 'c'  # no walk
    df.loc[df.subzone == 1, 'subzone'] = 'a'  # short walk
    df.loc[df.subzone == 2, 'subzone'] = 'b'  # long walk
    df['taz_sub'] = df.taz.astype('str') + df.subzone

    return df.set_index('taz_sub')


@orca.table(cache=False)
def accessibilities_segmentation(year, run_setup):

    if year in run_setup['logsum_period1']:
        df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                         "accessibility/travel_model/AccessibilityMarkets_{}.csv").format(run_setup['logsum_year1']))
    elif year in run_setup['logsum_period2']:
        df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                         "accessibility/travel_model/AccessibilityMarkets_{}.csv").format(run_setup['logsum_year2']))

    df['AV'] = df['hasAV'].apply(lambda x: 'AV' if x == 1 else 'noAV')
    df['label'] = (df['incQ_label'] + '_' + df['autoSuff_label'] + '_' + df['AV'])
    df = df.groupby('label').sum()
    df['prop'] = df['num_persons'] / df['num_persons'].sum()
    df = df[['prop']].transpose().reset_index(drop=True)

    return df


# shared between demolish and build tables below
def get_dev_projects_table():
    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), 
                                  "basis_inputs/parcels_buildings_agents/dev_pipeline.csv"))
    df = df.set_index("parcel_id")
    return df


@orca.table(cache=True)
def demolish_events():
    df = get_dev_projects_table()
    # keep demolish and build records
    # build records will be used to demolish the existing building on a parcel where a pipeline project is occuring
    # demolish events will be demolished
    return df[df.action.isin(["demolish", "build"])]


@orca.table(cache=True)
def development_projects():
    df = get_dev_projects_table()
    # keep add and build records
    # build records will be built on a parcel
    # add records will be added to a parcel where a building already exists
    df = df[df.action.isin(["add", "build"])]

    print("Describe of development projects")
    print(df[orca.get_table('buildings').local_columns].describe())

    return df


@orca.table(cache=True)
def dev_pipeline_strategy_projects(run_setup, development_projects):

    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/dev_pipeline_strategy_projects.csv"))
    df = df.set_index("parcel_id")

    if run_setup["dev_pipeline_strategy_projects"]:
        dp = development_projects.to_frame()
        # should error if the columns don't match the dev pipeline columns
        dp.append(df)
        # should all be add/build
        dp = dp[df.action.isin(["add", "build"])]

    return dp


@orca.table(cache=True)
def jobs(store):
    return store['jobs']


@orca.table(cache=True)
def households(store):
    return store['households']


@orca.table(cache=True)
def buildings(store):
    return store['buildings']


@orca.table(cache=True)
def residential_units(store)
    return store['residential_units']


@orca.table(cache=True)
def household_controls_unstacked():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "regional_controls/household_controls.csv"), index_col='year')


@orca.table(cache=True)
def tm1_tm2_regional_demographic_forecast():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm1_tm2_regional_demographic_forecast.csv"))


# the following overrides household_controls
# table defined in urbansim_defaults
@orca.table(cache=True)
def household_controls(household_controls_unstacked):
    df = household_controls_unstacked.to_frame()
    # rename to match legacy table
    df.columns = [1, 2, 3, 4]
    # stack and fill in columns
    df = df.stack().reset_index().set_index('year')
    # rename to match legacy table
    df.columns = ['base_income_quartile', 'total_number_of_households']
    return df


@orca.table(cache=True)
def employment_controls_unstacked():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "regional_controls/employment_controls.csv"), index_col='year')


@orca.table(cache=True)
def tm1_tm2_regional_controls():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm1_tm2_regional_controls.csv"), index_col="year")


@orca.table(cache=True)
def residential_vacancy_rate_mods():
    return pd.read_csv(os.path.join(misc.configs_dir(), "adjusters/residential_vacancy_rate_mods.csv"), index_col="year")


# the following overrides employment_controls
# table defined in urbansim_defaults
@orca.table(cache=True)
def employment_controls(employment_controls_unstacked):
    df = employment_controls_unstacked.to_frame()
    # rename to match legacy table
    df.columns = [1, 2, 3, 4, 5, 6]
    # stack and fill in columns
    df = df.stack().reset_index().set_index('year')
    # rename to match legacy table
    df.columns = ['empsix_id', 'number_of_jobs']
    return df


@orca.table(cache=True)
def tm1_taz1_forecast_inputs():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "zone_forecasts/tm1_taz1_forecast_inputs.csv"), 
                       dtype={'TAZ1454': np.int64, 'zone_id': np.int64})


# this is the set of categories by zone of sending and receiving zones
# in terms of vmt fees
@orca.table(cache=True)
def vmt_fee_categories():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/vmt_fee_zonecats.csv"), dtype={'taz': np.int64},
                       index_col="taz")


@orca.table(cache=True)
def sqft_per_job_adjusters(): 
    return pd.read_csv(os.path.join(misc.configs_dir(), "adjusters/sqft_per_job_adjusters.csv"), index_col="number")


@orca.table(cache=True)
def telecommute_sqft_per_job_adjusters(): 
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/telecommute_sqft_per_job_adjusters.csv"), index_col="number")


# these are shapes - "zones" in the bay area
@orca.table(cache=True)
def zones(store):
    # sort index so it prints out nicely when we want it to
    return store['zones'].sort_index()


# SLR progression by year
@orca.table(cache=True)
def slr_progression():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/hazards/slr_progression.csv"))


# SLR inundation levels for parcels
@orca.table(cache=True)
def slr_parcel_inundation():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/hazards/slr_parcel_inundation.csv"),
                       dtype={'parcel_id': np.int64}, index_col='parcel_id')


@orca.table(cache=True)
def slr_committed_migitation():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/hazards/slr_committed_mitigation.csv"),
                       index_col='parcel_id')


def slr_strategy_mitigation():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/hazards/slr_strategy_mitigation.csv"),
                       index_col='parcel_id')


# census tracts for parcels, to assign earthquake probabilities
@orca.table(cache=True)
def parcels_tract():
    return pd.read_csv(
        os.path.join(orca.get_injectable("inputs_dir"), "parcel_tract_xwalk.csv"),
        dtype={'parcel_id': np.int64,
               'zone_id':   np.int64},
        index_col='parcel_id')


# earthquake and fire damage probabilities for census tracts
@orca.table(cache=True)
def tracts_earthquake():
    return pd.read_csv(
        os.path.join(orca.get_injectable("inputs_dir"), "tract_damage_earthquake.csv"))


@orca.table(cache=True)
def employment_relocation_rates():
    df = pd.read_csv(os.path.join(misc.configs_dir(), "transition_relocation/employment_relocation_rates.csv"))
    df = df.set_index("zone_id")
    return df


@orca.table(cache=True)
def employment_relocation_rates_adjusters():
    df = pd.read_csv(os.path.join(misc.configs_dir(), "adjusters/employment_relocation_rates_overwrites.csv"))
    df = df.set_index("zone_id")
    return df


@orca.table(cache=True)
def household_relocation_rates():
    df = pd.read_csv(os.path.join(misc.configs_dir(), "transition_relocation/household_relocation_rates.csv"))
    return df


@orca.table(cache=True)
def renter_protections_relocation_rates():
    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/renter_protections_relocation_rates_overwrites.csv"))
    return df


@orca.table(cache=True)
def accessory_units():
    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "plan_strategies/accessory_units.csv"), index_col="juris")
    return df


@orca.table(cache=True)
def nodev_sites():
    df = pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/parcels_buildings_agents/nodev_sites.csv"), index_col="parcel_id")
    return df


# parcels-tract crosswalk that match the Urban Displacement Project census tract vintage
@orca.table(cache=True)
def parcel_tract_crosswalk():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/crosswalks/parcel_tract_crosswalk.csv"))


# Urban Displacement Project census tracts
@orca.table(cache=True)
def displacement_risk_tracts():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/equity/udp_2017results.csv"))


# Urban Displacement Project census tracts
@orca.table(cache=True)
def coc_tracts():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/equity/COCs_ACS2018_tbl_TEMP.csv"))


# Urban Displacement Project census tracts
@orca.table(cache=True)
def buildings_w_eq_codes():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/hazards/buildings_w_earthquake_codes.csv"))


# Urban Displacement Project census tracts
@orca.table(cache=True)
def eq_retrofit_lookup():
    return pd.read_csv(os.path.join(orca.get_injectable("inputs_dir"), "basis_inputs/hazards/building_eq_categories.csv"))


# this specifies the relationships between tables
orca.broadcast('buildings', 'residential_units', cast_index=True, onto_on='building_id')
orca.broadcast('residential_units', 'households', cast_index=True, onto_on='unit_id')
orca.broadcast('growth_geographies', 'buildings', cast_index=True, onto_on='parcel_id')
orca.broadcast('parcels', 'buildings', cast_index=True, onto_on='parcel_id')
# adding
orca.broadcast('buildings', 'households', cast_index=True, onto_on='building_id')
orca.broadcast('buildings', 'jobs', cast_index=True, onto_on='building_id')
# not defined in urbansim_Defaults
orca.broadcast('tmnodes', 'buildings', cast_index=True, onto_on='tmnode_id')
orca.broadcast('taz_geography', 'parcels', cast_index=True, onto_on='zone_id')