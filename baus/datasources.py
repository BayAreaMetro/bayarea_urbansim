from __future__ import print_function
import numpy as np
import pandas as pd
import os
from urbansim_defaults import datasources
from urbansim_defaults import utils
from urbansim.utils import misc
import orca
from baus import preprocessing
from baus.utils import geom_id_to_parcel_id, parcel_id_to_geom_id
from baus.utils import nearest_neighbor
import yaml



### MISC INJECTABLES ###

@orca.injectable('mapping', cache=True)
def mapping():
    with open(os.path.join(misc.configs_dir(), "mapping.yaml")) as f:
        return yaml.load(f)

# override the locations of settings defined in urbansim_defaults
@orca.injectable("building_type_map")
def building_type_map(mapping):
    return mapping["building_type_map"]

@orca.injectable('year')
def year():
    try:
        return orca.get_injectable("iter_var")
    except Exception as e:
        pass
    # if we're not running simulation, return base year
    return 2014

@orca.injectable()
def initial_year():
    return 2010

@orca.injectable()
def final_year():
    return 2050

@orca.injectable(cache=True)
def store(settings):
    return pd.HDFStore(os.path.join(misc.data_dir(), settings["store"]))

@orca.injectable(cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


### CONFIG FILES ###

@orca.injectable(cache=True)
def elcm_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_owner_config():
    return 

@orca.injectable(cache=True)
def hlcm_owner_no_unplaced_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_owner_lowincome_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_owner_lowincome_no_unplaced_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_renter_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_renter_no_unplaced_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_renter_lowincome_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def hlcm_renter_lowincome_no_unplaced_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def rsh_config():
    df = pd.read_csv(os.path.join(misc.data_dir()))
    orca.add_injectable("rsh_file", df)
    return get_config_file('rsh')

@orca.injectable(cache=True)
def rrh_config():
    return pd.read_csv(os.path.join(misc.data_dir()))

@orca.injectable(cache=True)
def nrh_config():
    return pd.read_csv(os.path.join(misc.data_dir()))


### BASIS INPUTS ###

@orca.injectable(cache=True)
def existing_inclusionary()
    return return os.path.join(misc.data_dir(), "existing_inclusionary.csv")

@orca.injectable(cache=True)
def existing_job_caps():
    return os.path.join(misc.data_dir(), "existing_job_caps.csv")

@orca.table(cache=True)
def parcels(store):
    df = store['parcels']
    return df

@orca.table(cache=True)
def parcels_zoning_calculations(parcels):
    return pd.DataFrame(index=parcels.index)

@orca.table()
def taz(zones):
    return zones

@orca.table(cache=True)
def parcels_geography(parcels, settings):
    file = os.path.join(misc.data_dir(), "parcels_geography.csv")
    df = pd.read_csv(file,
                     dtype={'PARCEL_ID':       np.int64,
                            'geom_id':         np.int64,
                            'jurisdiction_id': np.int64},
                     index_col="geom_id")
    return df

@orca.table(cache=True)
def development_projects(parcels): 
    return os.path.join(misc.data_dir(), "development_projects.csv")

@orca.table(cache=True)
def demolish_events(parcels, settings, development_projects):
    df = development_projects
    # keep demolish and build records
    return df[df.action.isin(["demolish", "build"])]

@orca.table(cache=True)
def jobs(store):
    return print_error_if_not_available(store, 'jobs_preproc')

@orca.table(cache=True)
def households(store):
    return print_error_if_not_available(store, 'households_preproc')

@orca.table(cache=True)
def buildings(store):
    return print_error_if_not_available(store, 'buildings_preproc')

@orca.table(cache=True)
def residential_units(store):
    return print_error_if_not_available(store, 'residential_units_preproc')

# key locations in the Bay Area for use as attractions in the models
@orca.table(cache=True)
def landmarks():
    return pd.read_csv(os.path.join(misc.data_dir(), 'landmarks.csv'),
                       index_col="name")


# BASE YEAR ZONING #

@orca.table(cache=True)
def zoning_lookup():
    file = os.path.join(misc.data_dir(), "2020_11_05_zoning_lookup_hybrid_pba50.csv")
    print('Version of zoning_lookup: {}'.format(file))
    return pd.read_csv(file,
                       dtype={'id': np.int64},
                       index_col='id')

@orca.table(cache=True)
def zoning_baseline(parcels, zoning_lookup, settings):
    file = os.path.join(misc.data_dir(), "2020_11_05_zoning_parcels_hybrid_pba50.csv")
    print('Version of zoning_parcels: {}'.format(file))                    
    df = pd.read_csv(file,
                     dtype={'geom_id':   np.int64,
                            'PARCEL_ID': np.int64,
                            'zoning_id': np.int64},
                     index_col="geom_id")
    df = pd.merge(df, zoning_lookup.to_frame(),
                  left_on="zoning_id", right_index=True)
    df = geom_id_to_parcel_id(df, parcels)

    return df

# BASE YEAR HAZARDS #

@orca.table(cache=True)
def slr_parcel_inundation():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_parcel_inundation.csv"),
        dtype={'parcel_id': np.int64},
        index_col='parcel_id')

@orca.table(cache=True)
def slr_progression():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_progression.csv"))

# earthquake and fire damage probabilities for census tracts
@orca.table(cache=True)
def tracts_earthquake():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "tract_damage_earthquake.csv"))

# census tract-parcel xwalk
@orca.table(cache=True)
def parcels_tract():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "parcel_tract_xwalk.csv"),
        dtype={'parcel_id': np.int64,
               'zone_id':   np.int64},
        index_col='parcel_id')



### PLAN POLICY INPUT FILES ###

@orca.table(cache=True)
def preservation():
    try:
      df = pd.read_csv("../inputs/plamn_policy/preservation.csv")
      orca.add_injectable("preservation_on", True)
    except Exception as e:
      orca.add_injectable("preservation_on", False)
      return
    return df

@orca.table(cache=True)
def ceqa_reform():
    try:
      df = pd.read_csv("../inputs/plan_policy/ceqa_reform.csv")
      orca.add_injectable("ceqa_reform_on", True)
    except Exception as e:
      orca.add_injectable("ceqa_reform_on", False)
      return
    return df

@orca.table(cache=True)
def parkings_reqs():
    try:
      df = pd.read_csv("../inputs/plan_policy/parking_requirements.csv")
      orca.add_injectable("parking_requirements_on", True)
    except Exception as e:
      orca.add_injectable("parking_requirements_on", False)
      return
    return df

@orca.table(cache=True)
def land_value_tax():
    try:
      df = pd.read_csv("../inputs/plan_policy/land_value_tax_settings.csv")
      orca.add_injectable("land_value_tax_on", True)
    except Exception as e:
      orca.add_injectable("land_value_tax_on", False)
      return
    return df

@orca.table(cache=True)
def sb743():
    try:
      df = pd.read_csv("../inputs/plan_policy/sb743_settings.csv")
      orca.add_injectable("sb743_on", True)
    except Exception as e:
      orca.add_injectable("sb743_on", False)
      return
    return df

@orca.injectable(cache=True)
def inclusionary_zoning():
    try:
      df = pd.read_csv("../inputs/plan_policy/inclusionary_zoning.csv")
      orca.add_injectable("inclusionary_zoning_on", True)
    except Exception as e:
      orca.add_injectable("inclusionary_zoning_on", False)
      return
    return df

@orca.table(cache=True)
def housing_bonds():
    try:
      df = pd.read_csv("../inputs/plan_policy/housing_bonds.csv")
      orca.add_injectable("housing_bonds_on", True)
    except Exception as e:
      orca.add_injectable("housing_bonds_on", False)
      return
    return df

@orca.table(cache=True)
def office_bonds():
    try:
      df = pd.read_csv("../inputs/plan_policy/vmt_fees.csv")
      orca.add_injectable("office_bonds_on", True)
    except Exception as e:
      orca.add_injectable("office_bonds_on", False)
      return
    return df

@orca.table(cache=True)
def vmt_fees():
    try:
      df = pd.read_csv("../inputs/_plan_policy/vmt_fees.csv")
      orca.add_injectable("vmt_fees_on", True)
    except Exception as e:
      orca.add_injectable("vmt_fees_on", False)
      return
    return df

@orca.table(cache=True)
def vmt_fee_categories():
    try:
      df = pd.read_csv("../inputs/plan_policy/vmt_fee_zonecats.csv")
    except Exception as e:
      return
    return df



@orca.table(cache=True)
def accessory_units():
    return pd.read_csv("data/accessory_units.csv", index_col="juris")



@orca.table(cache=True)
def renter_protections(policy):
    return pd.read_csv(os.path.join(misc.data_dir(), renter_protections_relocation_rates.csv))




@orca.table(cache=True)
def telework(): 
	df = pd.read_csv(os.path.join(misc.data_dir(), "superdistricts.csv"), index_col="number")
	return df

@orca.injectable(cache=True)
def job_caps_policy(jop):
    return os.path.join(misc.data_dir(), "job_caps_policy.csv")

@orca.table(cache=True)
def zoning_scenario(parcels_geography, mapping):

    df = pd.read_csv(os.path.join(misc.data_dir()))

    for k in mapping["building_type_map"].keys():
        df[k] = np.nan

    def add_drop_helper(col, val):
        for ind, item in df[col].items():
            if not isinstance(item, str):
                continue
            for btype in item.split():
                df.loc[ind, btype] = val

    add_drop_helper("add_bldg", 1)
    add_drop_helper("drop_bldg", 0)
                                              
    join_col = 'zoningmodcat'

    print('join_col of zoningmods is {}'.format(join_col))

    return pd.merge(parcels_geography.to_frame().reset_index(),
                    df,
                    on=join_col,
                    how='left').set_index('parcel_id')

@orca.table(cache=True)
def jobs_housing_fees():
    return pd.read_csv(os.path.join(misc.data_dir(), "jobs_housing_fees.csv"))

@orca.table(cache=True)
def slr_parcel_inundation_mitigation():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "slr_parcel_inundation_mitigation.csv"),
        dtype={'parcel_id': np.int64},
        index_col='parcel_id')

@orca.injectable(cache=True)
def obag_policy()
    return os.path.join(misc.data_dir(), "obag.csv")




### ZONAL FORECAST FILES ###

@orca.table(cache=True)
def zone_forecast_inputs():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "zone_forecast_inputs.csv"),
        dtype={'zone_id': np.int64},
        index_col="zone_id")

@orca.table(cache=True)
def taz_forecast_inputs():
    return pd.read_csv(
        os.path.join(misc.data_dir(), "taz_forecast_inputs.csv"),
        dtype={'TAZ1454': np.int64},
        index_col="TAZ1454")

@orca.table(cache=True)
def baseyear_taz_controls():
    return pd.read_csv(os.path.join("data", "baseyear_taz_controls.csv"), dtype={'taz1454': np.int64}, index_col="taz1454")


@orca.table(cache=True)
def base_year_summary_taz(mapping):
    df = pd.read_csv(os.path.join('output', 'baseyear_taz_summaries_2010.csv'), dtype={'taz1454': np.int64}, index_col="zone_id")
    cmap = mapping["county_id_tm_map"]
    df['COUNTY_NAME'] = df.COUNTY.map(cmap)
    return df

@orca.table(cache=True)
def taz2_forecast_inputs(regional_demographic_forecast):
    df = pd.read_csv(os.path.join(misc.data_dir(), "taz2_forecast_inputs.csv"), dtype={'TAZ': np.int64},
                     index_col='TAZ').replace('#DIV/0!', np.nan)
    return df

@orca.table(cache=True)
def maz_forecast_inputs(regional_demographic_forecast):
    df = pd.read_csv(os.path.join(misc.data_dir(), "maz_forecast_inputs.csv"), dtype={'MAZ': np.int64},
                     index_col='MAZ').replace('#DIV/0!', np.nan)
    return df

@orca.table(cache=True)
def empsh_to_empsix():
    return pd.read_csv(os.path.join(misc.data_dir(), "empsh_to_empsix.csv"))



### REGIONAL CONTROLS ###

@orca.table(cache=True)
def county_forecast_inputs():
    return pd.read_csv(os.path.join(misc.data_dir(), "county_forecast_inputs.csv"), index_col="COUNTY")

@orca.table(cache=True)
def county_employment_forecast():
    return pd.read_csv(os.path.join(misc.data_dir(), "county_employment_forecast.csv"))

@orca.table(cache=True)
def regional_demographic_forecast():
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("reg_dem_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname))

@orca.table(cache=True)
def household_controls_unstacked():
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("household_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname), index_col='year')

# the following overrides household_controls table defined in urbansim_defaults
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
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("employment_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname), index_col='year')

@orca.table(cache=True)
def regional_controls():
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("reg_control_file", fname)
    return pd.read_csv(os.path.join('data', fname), index_col="year")

# the following overrides employment_controls table defined in urbansim_defaults
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
def household_controls_unstacked():
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("household_control_file", fname)
    return pd.read_csv(os.path.join(misc.data_dir(), fname), index_col='year')



### TRAVEL ACCESSIBILITY ###

@orca.table(cache=False)
def mandatory_accessibility(year):
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("mand_acc_file_2010", df)
    df.loc[df.subzone == 0, 'subzone'] = 'c'  # no walk
    df.loc[df.subzone == 1, 'subzone'] = 'a'  # short walk
    df.loc[df.subzone == 2, 'subzone'] = 'b'  # long walk
    df['taz_sub'] = df.taz.astype('str') + df.subzone
    return df.set_index('taz_sub')

@orca.table(cache=False)
def non_mandatory_accessibility(year):
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
	orca.add_injectable("nonmand_acc_file_2010", df)
    df.loc[df.subzone == 0, 'subzone'] = 'c'  # no walk
    df.loc[df.subzone == 1, 'subzone'] = 'a'  # short walk
    df.loc[df.subzone == 2, 'subzone'] = 'b'  # long walk
    df['taz_sub'] = df.taz.astype('str') + df.subzone
    return df.set_index('taz_sub')

@orca.table(cache=False)
def accessibilities_segmentation(year):
    df = pd.read_csv(os.path.join(misc.data_dir(), year))
    orca.add_injectable("acc_seg_file_2010", df)
    df['AV'] = df['hasAV'].apply(lambda x: 'AV' if x == 1 else 'noAV')
    df['label'] = (df['incQ_label'] + '_' + df['autoSuff_label'] + '_' + df['AV'])
    df = df.groupby('label').sum()
    df['prop'] = df['num_persons'] / df['num_persons'].sum()
    df = df[['prop']].transpose().reset_index(drop=True)
    return df



### ADJUSTERS ###

@orca.table(cache=True)
def taz2_price_shifters():
    return pd.read_csv(os.path.join(misc.data_dir(), "taz2_price_shifters.csv"), dtype={'TAZ': np.int64}, index_col="TAZ")

@orca.table(cache=True)
def taz_household_relocation_():
    return pd.read_csv(os.path.join(misc.data_dir(), "taz_household_relocation.csv"), dtype={'TAZ': np.int64}, index_col="TAZ")

@orca.table(cache=True)
def taz_employment_relocation():
    df = pd.read_csv(os.path.join(misc.data_dir(), "employment_relocation_rates.csv"))
    df = df.set_index("zone_id").stack().reset_index()
    df.columns = ["zone_id", "empsix", "rate"]
    return df



### BROADCASTS ###

# this specifies the relationships between tables
orca.broadcast('buildings', 'residential_units', cast_index=True, onto_on='building_id')
orca.broadcast('residential_units', 'households', cast_index=True, onto_on='unit_id')
orca.broadcast('parcels_geography', 'buildings', cast_index=True, onto_on='parcel_id')
orca.broadcast('parcels', 'buildings', cast_index=True, onto_on='parcel_id')
# not defined in urbansim_defaults
orca.broadcast('tmnodes', 'buildings', cast_index=True, onto_on='tmnode_id')
orca.broadcast('taz_geography', 'parcels', cast_index=True, onto_on='zone_id')