from __future__ import print_function

import pathlib
import orca
import pandas as pd
import numpy as np


@orca.step()
def adjust_initial_summary_year_incomes(households, initial_summary_year_taz_controls, year, initial_summary_year):

    if year != initial_summary_year:
        return
    
    households = households.to_frame()
    taz_controls = initial_summary_year_taz_controls.to_frame()

    for taz in initial_summary_year_taz_controls.index:
        # select the tazdata for a taz
        tazdata = taz_controls.iloc[taz]
        # select all households in that taz
        hhs_in_taz = households[households.taz == tazdata.ZONE].index

        hhs_to_update = hhs_in_taz.copy()
        for inc_quartile in [1, 2, 3, 4]:
            # use the taz controls to calculate the proportion of households in an income quartile
            prop = (tazdata['HHINCQ'+str(inc_quartile)]/tazdata['TOTHH'])
            # use the total number of HHs in the TAZ to calculate the number of HHs that should be in the income group
            if prop > 0:
                hh_target = (len(hhs_in_taz) * prop).astype(int)
                # randomly select households to assign to the income groups using the target number
                hhs_for_inc_quartile = (np.random.choice(hhs_to_update, hh_target, replace=False))
                # update households in the taz with their new income group
                households.loc[households.household_id.isin(hhs_for_inc_quartile), 'base_income_quartile'] = inc_quartile
                # remove the updated households from the set of households in the taz to be updated
                hhs_to_update = hhs_to_update[~hhs_to_update.isin(hhs_for_inc_quartile)]

    # save the final table of households with updated incomes
    households = orca.add_table("households", households)


@orca.step()
def parcel_summary(run_name, parcels, buildings, households, jobs, year, initial_summary_year, interim_summary_year, final_year):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
         return

    df = parcels.to_frame(["geom_id", "x", "y"])
    # add building data for parcels
    building_df = orca.merge_tables('buildings', [parcels, buildings], columns=['parcel_id', 'residential_units', 'deed_restricted_units',
                                                                                'preserved_units', 'inclusionary_units', 'subsidized_units',
                                                                                'non_residential_sqft'])
    for col in building_df.columns:
        if col == 'parcel_id':
            continue
        df[col] = building_df.groupby('parcel_id')[col].sum()

    # add households by quartile on each parcel
    households_df = orca.merge_tables('households', [buildings, households], columns=['parcel_id', 'base_income_quartile'])
    for i in range(1, 5):
        df['hhq%d' % i] = households_df[households_df.base_income_quartile == i].parcel_id.value_counts()
    df["tothh"] = households_df.groupby('parcel_id').size()

    # add jobs by empsix category on each parcel
    jobs_df = orca.merge_tables('jobs', [buildings, jobs], columns=['parcel_id', 'empsix'])
    for cat in jobs_df.empsix.unique():
        df[cat] = jobs_df[jobs_df.empsix == cat].parcel_id.value_counts()
    df["totemp"] = jobs_df.groupby('parcel_id').size()

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"{run_name}_parcel_summary_{year}.csv")


@orca.step()
def parcel_growth_summary(year, run_name, initial_summary_year, final_year):
    
    if year != final_year:
        return

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    initial_parcel_file = coresum_output_dir / f"{run_name}_parcel_summary_{initial_summary_year}.csv"
    print(f"initial_parcel_file resolve:{initial_parcel_file.resolve()} exists:{initial_parcel_file.exists()}")

    df1 = pd.read_csv(initial_parcel_file, index_col="parcel_id")
    df2 = pd.read_csv(coresum_output_dir / f"{run_name}_parcel_summary_{final_year}.csv",
                      index_col="parcel_id")

    for col in df1.columns:
        if col in ["geom_id", "x", "y"]:
            continue

        # fill na with 0 otherwise it drops the parcel data during subtraction
        df1[col].fillna(0, inplace=True)
        df2[col].fillna(0, inplace=True)

        df1[col] = df2[col] - df1[col]

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df1.to_csv(coresum_output_dir / f"{run_name}_parcel_growth.csv")
    


@orca.step()
def building_summary(run_name, parcels, buildings, year, initial_summary_year, final_year, interim_summary_year):

    if year not in [initial_summary_year, interim_summary_year, final_year]:
        return

    df = orca.merge_tables('buildings',
        [parcels, buildings],
        columns=['parcel_id', 'year_built', 'building_type', 'residential_units', 'unit_price', 
                 'non_residential_sqft', 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces', 'source'])

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"{run_name}_building_summary_{year}.csv")


@orca.step()
def new_buildings_summary(run_name, parcels, buildings, year, final_year):

    if year != final_year:
        return

    df = orca.merge_tables('buildings', [parcels, buildings])    
    df = df[~df.source.isin(["h5_inputs"])]

    df = df[['parcel_id', 'building_type', 'building_sqft', 'deed_restricted_units', 'year_built',
             'preserved_units', 'inclusionary_units', 'subsidized_units',
             'non_residential_sqft', 'residential_price', 'residential_units', 'source',	
             'vacant_residential_units', 'vacant_job_spaces', 'vacant_res_units', 'price_per_sqft',	'unit_price',	
             'land_value',	'acres', 'x', 'y', 'parcel_acres', 'total_residential_units',	'total_job_spaces',	
             'zoned_du', 'zoned_du_underbuild', 'sdem', 'pda_id', 'cat_id',	'tra_id', 'sesit_id', 'ppa_id',	'coc_id',	
             'urbanized', 'manual_nodev', 'total_non_residential_sqft',	'nodev',	
             'built_far', 'max_far', 'built_dua', 'max_dua', 'building_purchase_price_sqft',	
             'building_purchase_price',	'land_cost', 'slr_nodev']]

    df["run_name"] = run_name

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"{run_name}_new_buildings_summary.csv")


@orca.step()
def interim_zone_output(run_name, households, buildings, residential_units, parcels, jobs, zones, year,
                        initial_summary_year, final_year):

    # TODO: currently TAZ, do we want this to be MAZ?
    zones = pd.DataFrame(index=zones.index)
    
    parcels = parcels.to_frame()
    parcels["zone_id_x"] = parcels.zone_id
    orca.add_table('parcels', parcels)
    parcels = orca.get_table("parcels")

    households = orca.merge_tables('households', 
                                   [parcels, buildings, households], columns=['zone_id', 'zone_id_x', 'base_income_quartile'])
    households["zone_id"] = households.zone_id_x
    
    jobs = orca.merge_tables('jobs', [parcels, buildings, jobs], columns=['zone_id', 'zone_id_x', 'empsix'])
    jobs["zone_id"] = jobs.zone_id_x

    parcels = parcels.to_frame()
    buildings = buildings.to_frame()
    residential_units = residential_units.to_frame()

    zones['non_residential_sqft'] = buildings.groupby('zone_id').non_residential_sqft.sum()
    zones['job_spaces'] = buildings.groupby('zone_id').job_spaces.sum()
    
    zones['residential_units'] = buildings.groupby('zone_id').residential_units.sum()
    zones["deed_restricted_units"] = buildings.groupby('zone_id').deed_restricted_units.sum()
    zones["preserved_units"] = buildings.groupby('zone_id').preserved_units.sum()
    zones["inclusionary_units"] = buildings.groupby('zone_id').inclusionary_units.sum()
    zones["subsidized_units"] = buildings.groupby('zone_id').subsidized_units.sum()

    # CAPACITY
    zones['zoned_du'] = parcels.groupby('zone_id').zoned_du.sum()
    zones['zoned_du_underbuild'] = parcels.groupby('zone_id').zoned_du_underbuild.sum()
    zones['zoned_du_underbuild_ratio'] = zones.zoned_du_underbuild / zones.zoned_du

    # VACANCY
    tothh = households.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['residential_vacancy'] = 1.0 - tothh / zones.residential_units.replace(0, 1)
    totjobs = jobs.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['non_residential_vacancy'] = 1.0 - totjobs / zones.job_spaces.replace(0, 1)

    # PRICE VERSUS NONRES RENT
    zones['residential_price'] = residential_units.groupby('zone_id').unit_residential_price.quantile()
    zones['residential_rent'] = residential_units.groupby('zone_id').unit_residential_rent.quantile()
    zones['non_residential_rent'] = buildings.groupby('zone_id').non_residential_rent.quantile()

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    zones.to_csv(coresum_output_dir / f"{run_name}_interim_zone_output_{year}.csv")

    # now add all interim zone output to a single dataframe

    zones = zones.add_suffix("_"+str(year))

    try:
        all_years = orca.get_table("interim_zone_output_all").to_frame()
    except KeyError:
        all_years = pd.DataFrame(index=zones.index)
        
    all_years = all_years.merge(zones, left_index=True, right_index=True)
    orca.add_table("interim_zone_output_all", all_years)

    if year == final_year:
        coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
        coresum_output_dir.mkdir(parents=True, exist_ok=True)
        all_years.to_csv(coresum_output_dir / f"{run_name}_interim_zone_output_allyears.csv")
