from __future__ import print_function

import pathlib
import orca
import pandas as pd


@orca.step()
def parcel_summary(run_name, parcels, buildings, households, jobs, year, initial_summary_year, final_year, interim_summary_years):

    if year not in [initial_summary_year, final_year] + interim_summary_years:
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
    df.to_csv(coresum_output_dir / f"parcel_summary_{year}.csv")


@orca.step()
def parcel_growth_summary(year, run_name, initial_summary_year, final_year):
    
    if year != final_year:
        return

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    initial_parcel_file = coresum_output_dir / f"parcel_summary_{initial_summary_year}.csv"
    print(f"initial_parcel_file resolve:{initial_parcel_file.resolve()} exists:{initial_parcel_file.exists()}")

    df1 = pd.read_csv(initial_parcel_file, index_col="parcel_id")
    df2 = pd.read_csv(coresum_output_dir / f"parcel_summary_{final_year}.csv",
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
    df1.to_csv(coresum_output_dir / f"parcel_growth.csv")
    


@orca.step()
def building_summary(run_name, parcels, buildings, year, initial_summary_year, final_year, interim_summary_years):

    if year not in [initial_summary_year, final_year] + interim_summary_years:
        return

    df = orca.merge_tables('buildings',
        [parcels, buildings],
        columns=['parcel_id', 'year_built', 'building_type', 'residential_units', 'unit_price', 
                 'non_residential_sqft', 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces', 'source'])

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"building_summary_{year}.csv")


@orca.step()
def new_buildings_summary(run_name, parcels, parcels_zoning_calculations, buildings, year, final_year):

    if year != final_year:
        return

    parcels = parcels.to_frame().join(parcels_zoning_calculations.to_frame(), lsuffix='parcels')
    df = buildings.to_frame().merge(parcels, left_on="parcel_id", right_index=True)

    df = df[~df.source.isin(["h5_inputs"])] 

    df = df[['parcel_id', 'building_type', 'building_sqft', 'deed_restricted_units', 'year_built',
             'preserved_units', 'inclusionary_units', 'subsidized_units',
             'non_residential_sqft', 'residential_price', 'residential_units', 'source',	
             'vacant_residential_units', 'vacant_job_spaces', 'vacant_res_units', 'price_per_sqft',	'unit_price',	
             'land_value',	'acres', 'x', 'y', 'parcel_acres', 'total_residential_units',	'total_job_spaces',	
             'zoned_du', 'zoned_du_underbuild', 'zoned_du_build_ratio', 'zoned_far', 'zoned_far_underbuild', 
             'zoned_far_build_ratio', 'sdem',	
             'urbanized', 'manual_nodev', 'total_non_residential_sqft',	'nodev',	
             'built_far', 'max_far', 'built_dua', 'max_dua', 'building_purchase_price_sqft',	
             'building_purchase_price',	'land_cost', 'slr_nodev']]

    df["run_name"] = run_name

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"new_buildings_summary.csv")


@orca.step()
def interim_zone_output(run_name, households, buildings, residential_units, parcels, jobs, zones, year,
                        parcels_zoning_calculations, initial_summary_year, final_year):

    # TODO: currently TAZ, do we want this to be MAZ?
    zones = pd.DataFrame(index=zones.index)
    
    parcels = parcels.to_frame()
    parcels["zone_id_x"] = parcels.zone_id
    orca.add_table('parcels', parcels)
    parcels = orca.get_table("parcels")

    households = orca.merge_tables('households', 
                                   [parcels, buildings, households], columns=['zone_id', 'zone_id_x', 'base_income_quartile'])
    households["zone_id"] = households.zone_id_x
    
    jobs = orca.merge_tables('jobs', [parcels, buildings, jobs],
                         columns=['zone_id', 'zone_id_x', 'empsix', "ec5_cat"])
    jobs["zone_id"] = jobs.zone_id_x
    jobs['is_transit_hub'] = (jobs.ec5_cat=="Transit_Hub").map({True:'job_in_transit_hub',False:'job_not_in_transit_hub'})

    parcels = parcels.to_frame()
    parcels = parcels.join(parcels_zoning_calculations.to_frame(), lsuffix='parcels')

    buildings = buildings.to_frame()
    residential_units = residential_units.to_frame()

    # ADD JOBS BY TRANSIT ZONES
    jobs_by_ec5 = jobs.groupby(['zone_id','is_transit_hub']).size().unstack(1).fillna(0).astype(int)
    zones = zones.merge(jobs_by_ec5, how='left',left_index=True,right_index=True)

    zones['non_residential_sqft'] = buildings.groupby('zone_id').non_residential_sqft.sum()
    zones['non_residential_sqft_office'] = buildings.query('building_type=="OF"').groupby('zone_id').non_residential_sqft.sum()
    zones['job_spaces'] = buildings.groupby('zone_id').job_spaces.sum()
    zones['job_spaces_office'] = buildings.query('building_type=="OF"').groupby('zone_id').job_spaces.sum()
    
    zones['residential_units'] = buildings.groupby('zone_id').residential_units.sum()
    zones["deed_restricted_units"] = buildings.groupby('zone_id').deed_restricted_units.sum()
    zones["preserved_units"] = buildings.groupby('zone_id').preserved_units.sum()
    zones["inclusionary_units"] = buildings.groupby('zone_id').inclusionary_units.sum()
    zones["subsidized_units"] = buildings.groupby('zone_id').subsidized_units.sum()

    # CAPACITY
    zones['zoned_du'] = parcels.groupby('zone_id').zoned_du.sum()
    zones['zoned_du_underbuild'] = parcels.groupby('zone_id').zoned_du_underbuild.sum()
    zones['zoned_du_build_ratio'] = zones.residential_units / zones.zoned_du
    zones['zoned_far'] = parcels.groupby('zone_id').zoned_far.sum()
    zones['zoned_far_underbuild'] = parcels.groupby('zone_id').zoned_far_underbuild.sum()
    zones['zoned_far_build_ratio'] = zones.non_residential_sqft / zones.zoned_far

    # VACANCY
    tothh = households.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['residential_vacancy'] = 1.0 - tothh / zones.residential_units.replace(0, 1)
    totjobs = jobs.zone_id.value_counts().reindex(zones.index).fillna(0)
    zones['non_residential_vacancy'] = 1.0 - totjobs / zones.job_spaces.replace(0, 1)

    # office vacancy
    # note that the rate is calculated using spaces, not square feet, consistent
    # with how vacancy is calculated for non_residential_vacancy, leading to some 
    # modest loss of precision
    zones['non_residential_vacancy_office'] = (buildings.query('building_type=="OF"')
                                                .groupby(['zone_id'])
                                                .apply(lambda x: x['vacant_job_spaces'].sum().clip(0) /
                                                x['job_spaces'].sum().clip(1))
                                                )

    # PRICE VERSUS NONRES RENT
    zones['residential_price'] = residential_units.groupby('zone_id').unit_residential_price.quantile()
    zones['residential_rent'] = residential_units.groupby('zone_id').unit_residential_rent.quantile()
    zones['non_residential_rent'] = buildings.groupby('zone_id').non_residential_rent.quantile()

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    zones.to_csv(coresum_output_dir / f"interim_zone_output_{year}.csv")

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
        all_years.to_csv(coresum_output_dir / f"interim_zone_output_allyears.csv")
