from __future__ import print_function

import pathlib
import orca
import pandas as pd
from baus import datasources
import logging

# Get a logger specific to this module
logger = logging.getLogger(__name__)

@orca.step()
def parcel_transitions(parcels, year, initial_summary_year, final_year, run_name):
    """
    This function analyzes changes in building types at the parcel level between two dataframes.

    Args:
        buildings_start (pd.DataFrame): A pandas DataFrame containing building data at the starting time period (typically 2020).
            This DataFrame is expected to have columns including 'parcel_id', 'building_type', 'residential_units',
            and 'non_residential_sqft' (or similar column names).
        buildings_end (pd.DataFrame): A pandas DataFrame containing building data at the ending time period (typically 2050).
            This DataFrame is expected to have the same columns as buildings_start.

    Returns:
        pd.DataFrame: A new DataFrame containing information about building transitions between the two time periods.
            The DataFrame includes columns like 'parcel_id', 'building_type_add' (building type added),
            'building_type_demo' (building type demolished), 'transition_type' (a string describing the transition),
            and potentially other columns depending on the input DataFrames.
    """

    
    if year!=final_year:
        print(f'Skipping redevelopment tallying for year {year}')
        return
    
    print('Loading type mappings')
    # get building types list
    mapping = orca.get_injectable("mapping")
    form_to_btype = mapping["form_to_btype"]
    
    # explode and generalize
    building_type_to_general_type = pd.Series(form_to_btype).explode().reset_index(name='building_type')
    building_type_to_general_type = building_type_to_general_type.set_index('building_type')['index']
    
    # recode a few items for a bit more detail
    building_type_to_general_type.loc['HM']='residential-multi'
    building_type_to_general_type.loc['HS']='residential-single'
    building_type_to_general_type = building_type_to_general_type[building_type_to_general_type!='select_non_residential']

    print('Loading buildings: ')
    # get buildings - first and last year data
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    
    # if we're already reading disaggregate output files, shouldn't this just be a post-process rather than a model step?
    buildings_start_path = coresum_output_dir / f"building_table_{initial_summary_year}.csv"
    print(f'Loading {buildings_start_path}')
    
    buildings_start = pd.read_csv(buildings_start_path, 
        index_col='building_id')
    
    buildings_end_path = coresum_output_dir / f"building_table_{final_year}.csv"
    print(f'Loading {buildings_end_path}')
    
    buildings_end = pd.read_csv(buildings_end_path, 
        index_col='building_id')
    
    # assign generalized building type
    buildings_start['building_type_gen'] = buildings_start['building_type'].map(building_type_to_general_type)
    buildings_end['building_type_gen'] = buildings_end['building_type'].map(building_type_to_general_type)
    
    parcels_df = parcels.to_frame(['county','superdistrict','subregion'])
    
    # store new csv here
    redev_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "redevelopment_summaries"
    redev_output_dir.mkdir(parents=True, exist_ok=True)

    # Assuming an average unit size of 1500 sq ft
    AVG_UNIT_SIZE = 1500

    # Find building ids added between the two time periods
    building_additions_ix = buildings_end.index.difference(buildings_start.index)
    building_additions = buildings_end.loc[building_additions_ix]
    print(f'New buildings added between {initial_summary_year} and {final_year}: {building_additions_ix.shape[0]:,}')
    print('buildings_additions coding: ', building_additions.groupby(['building_type_gen','building_type']).size())

    # Find building ids removed (or demolished) between the two time periods
    building_demolitions_ix = buildings_start.index.difference(buildings_end.index)
    building_demolitions = buildings_start.loc[building_demolitions_ix]
    print(f'Old buildings removed between {initial_summary_year} and {final_year}: {building_demolitions_ix.shape[0]:,}')
    print('buildings_demolitions coding: ', building_demolitions.groupby(['building_type_gen','building_type']).size())

    # Calculate total square footage for demolished buildings based on unit size and non-residential area
    building_demolitions['residential_sqft'] = building_demolitions['residential_units'] * AVG_UNIT_SIZE
    building_demolitions['building_sqft'] = building_demolitions['residential_sqft'] + \
        building_demolitions['non_residential_sqft']

    # Find the largest use type (by sqft) among demolished buildings on each parcel
    building_demolitions_by_parcel = (
        building_demolitions.groupby(['parcel_id', 'building_type_gen'])[
            'building_sqft']
        .sum()
        .reset_index(1)
        .groupby(['parcel_id'])
        .first()
    )

    # Calculate total square footage for added buildings based on unit size and non-residential area
    building_additions['residential_sqft'] = building_additions['residential_units'] * AVG_UNIT_SIZE
    building_additions['building_sqft'] = building_additions['residential_sqft'] + \
        building_additions['non_residential_sqft']

    # this should be redundant since it was done on buildings_end but had persistent NaNs 
    # remove after testing
    building_additions['building_type_gen'] = building_additions.building_type.map(building_type_to_general_type)
    print('buildings_additions coding: ', building_additions.groupby(['building_type_gen','building_type']).size())

    # Find the largest use type (by sqft) among added buildings on each parcel
    # actually we are just getting the first one - nlargest was uncomfortably slow here.
    building_additions_by_parcel = (
        building_additions.groupby(['parcel_id', 'building_type_gen'])['building_sqft']
        .sum()
        .reset_index(1)
        .groupby(['parcel_id'])
        .first()
    )

    # Merge DataFrames containing information about added and demolished buildings
    # We keep the focus on buildings standing at the end year - some of them
    # may not be associated with demolitions at all

    joined = building_additions_by_parcel.merge(
        building_demolitions_by_parcel,
        left_index=True,
        right_index=True,
        suffixes=('_add', '_demo'),
        how='left',
    )

    # Define a quick function to determine the transition type based on the
    # entering / exiting buildings for a parcel. Just a tad too long for lambda.

    def transition_state_string(row):
        if row.building_type_gen_demo == row.building_type_gen_add:
            return row.building_type_gen_demo
        else:
            return f'{row.building_type_gen_demo}->{row.building_type_gen_add}'

    joined.building_type_gen_demo = joined.building_type_gen_demo.fillna('VAC')

    # Apply the transition_state_string function (assumed to be defined elsewhere) to each row to determine the transition type
    joined['transition_type'] = joined.apply(transition_state_string, axis=1)

    for geography in ['county','superdistrict','subregion']:
        # add a geo classifier to the df
        joined[geography] = parcels_df[geography]
    
        out = joined.groupby([geography,'transition_type']).building_sqft_demo.sum().unstack('transition_type').fillna(0)
        out_path = redev_output_dir / f"{geography}_redev_summary_growth.csv"
        out.to_csv(out_path)


@orca.step()
def geographic_summary(parcels, households, jobs, buildings, year, superdistricts_geography,
                       initial_summary_year, final_year, interim_summary_years, run_name):  

    # Commenting this out so we get geographic summaries for all years - DSL 2023-08-31
    # if year not in [initial_summary_year, final_year] + interim_summary_years:
    #      return

    households_df = orca.merge_tables('households', [parcels, buildings, households],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'base_income_quartile',])

    jobs_df = orca.merge_tables('jobs', [parcels, buildings, jobs],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'empsix', 'ec5_cat'])

    buildings_df = orca.merge_tables('buildings', [parcels, buildings],
        columns=['juris', 'superdistrict', 'county', 'subregion', 'building_type', 
                 'residential_units', 'deed_restricted_units', 'non_residential_sqft','job_spaces',
                 'vacant_job_spaces'])

    jobs_df['is_transit_hub'] = (jobs_df.ec5_cat=="Transit_Hub").map({True:'job_in_transit_hub',False:'job_not_in_transit_hub'})
    

    #### summarize regional results ####
    region = pd.DataFrame(index=['region'])
    region.index.name = 'region'

    # households
    region['tothh'] = households_df.size
    for quartile in [1, 2, 3, 4]:
        region['hhincq'+str(quartile)] = households_df[households_df.base_income_quartile == quartile].size

    # employees by sector
    region['totemp'] = jobs_df.size
    for empsix in [ 'AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
        region[empsix] = jobs_df[jobs_df.empsix == empsix].size

    # residential buildings
    region['residential_units'] = buildings_df.residential_units.sum() 
    region['deed_restricted_units'] = buildings_df.deed_restricted_units.sum()  
    region['sfdu'] = buildings_df[(buildings_df.building_type == 'HS') | (buildings_df.building_type == 'HT')].residential_units.sum()
    region['mfdu'] = buildings_df[(buildings_df.building_type == 'HM') | (buildings_df.building_type == 'MR')].residential_units.sum()
    
    # non-residential buildings
    region['non_residential_sqft'] = buildings_df.non_residential_sqft.sum()
    geosum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "geographic_summaries"
    geosum_output_dir.mkdir(parents=True, exist_ok=True)
    region.to_csv(geosum_output_dir / f"region_summary_{year}.csv")

    #### summarize by sub-regional geography ####
    geographies = ['juris', 'superdistrict', 'county', 'subregion']

    for geography in geographies:

        # remove rows with null geography- seen with "county"
        
        # TODO: we should start with the raw buildings frame - so we don't
        # end up increasingly reducing the frame if there are NAs
        # in an early loop carrying through to later loops.

        buildings_df = buildings_df[~pd.isna(buildings_df[geography])]
        households_df = households_df[~pd.isna(households_df[geography])]
        jobs_df = jobs_df[~pd.isna(jobs_df[geography])]

        summary_table = pd.DataFrame(index=buildings_df[geography].unique())
        
        # add superdistrict name 
        if geography == 'superdistrict':
            superdistricts_geography = superdistricts_geography.to_frame()
            summary_table = summary_table.merge(superdistricts_geography[['name']], left_index=True, right_index=True)

        # households
        summary_table['tothh'] = households_df.groupby(geography).size()
        for quartile in [1, 2, 3, 4]:
            summary_table['hhincq'+str(quartile)] = households_df[households_df.base_income_quartile == quartile].groupby(geography).size()

        # residential buildings
        summary_table['residential_units'] = buildings_df.groupby(geography).residential_units.sum() 
        summary_table['deed_restricted_units'] = buildings_df.groupby(geography).deed_restricted_units.sum() 
        summary_table['sfdu'] = buildings_df[(buildings_df.building_type == 'HS') | (buildings_df.building_type == 'HT')].\
            groupby(geography).residential_units.sum()
        summary_table['mfdu'] = buildings_df[(buildings_df.building_type == 'HM') | (buildings_df.building_type == 'MR')].\
            groupby(geography).residential_units.sum()
        
        # employees by sector
        summary_table['totemp'] = jobs_df.groupby(geography).size()
        for empsix in ['AGREMPN', 'MWTEMPN', 'RETEMPN', 'FPSEMPN', 'HEREMPN', 'OTHEMPN']:
            summary_table[empsix] = jobs_df[jobs_df.empsix == empsix].groupby(geography).size()
        summary_table['transit_hub_jobs'] = jobs_df.query('ec5_cat=="Transit_Hub" ').groupby(geography).size()
    

        # non-residential buildings
        summary_table['non_residential_sqft'] = buildings_df.groupby(geography)['non_residential_sqft'].sum().round(0)
        summary_table['non_residential_sqft_office'] = buildings_df.query('building_type=="OF"').groupby(geography)['non_residential_sqft'].sum().round(0)
        
        summary_table['job_spaces'] = buildings_df.groupby(geography)['job_spaces'].sum().round(0)
        summary_table['job_spaces_vacant'] = buildings_df.groupby(geography)['vacant_job_spaces'].sum().round(0)
        
        summary_table['job_spaces_office'] = buildings_df.query('building_type=="OF"').groupby(geography)['job_spaces'].sum().round(0)
        summary_table['job_spaces_office_vacant'] = buildings_df.query('building_type=="OF"').groupby(geography)['vacant_job_spaces'].sum().round(0)
        
        summary_table['job_spaces_vacant_pct'] = summary_table['job_spaces_vacant'] / summary_table['job_spaces'].clip(1)
        summary_table['job_spaces_office_vacant_pct']= summary_table['job_spaces_office_vacant'] / summary_table['job_spaces_office'].clip(1)

        summary_table.index.name = geography
        summary_table = summary_table.sort_index()
        summary_table.fillna(0).to_csv(geosum_output_dir / f"{geography}_summary_{year}.csv")

# office vacancy
    # note that the rate is calculated using spaces, not square feet, consistent
    # with how vacancy is calculated for non_residential_vacancy, leading to some 
    # modest loss of precision
    # zones['non_residential_vacancy_office'] = (buildings.query('building_type=="OF"')
    #                                             .groupby(['zone_id'])
    #                                             .apply(lambda x: x['vacant_job_spaces'].sum().clip(0) /
    #                                             x['job_spaces'].sum().clip(0))
    #                                             )

@orca.step()
def geographic_growth_summary(year, final_year, initial_summary_year, run_name):
    
    if year != final_year: 
        return

    geographies = ['region', 'juris', 'superdistrict', 'county', 'subregion']
    geosum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "geographic_summaries"
    geosum_output_dir.mkdir(parents=True, exist_ok=True)

    for geography in geographies:

        # use 2015 as the base year
        year1 = pd.read_csv(geosum_output_dir / f"{geography}_summary_{initial_summary_year}.csv")
        year2 = pd.read_csv(geosum_output_dir / f"{geography}_summary_{final_year}.csv")

        geog_growth = year1.merge(year2, on=geography, suffixes=("_"+str(initial_summary_year), "_"+str(final_year)))

        geog_growth["run_name"] = run_name

        if geography == 'superdistrict':
            geog_growth = geog_growth.rename(columns={"name_"+(str(initial_summary_year)): "name"})
            geog_growth = geog_growth.drop(columns=["name_"+(str(final_year))])
        
        columns = ['tothh', 'totemp', 'residential_units', 'deed_restricted_units', 'non_residential_sqft']
    
        for col in columns:
            # growth in households/jobs/etc.
            geog_growth[col+"_growth"] = (geog_growth[col+"_"+str(final_year)] - 
                                          geog_growth[col+"_"+str(initial_summary_year)])

            # percent change in geography's households/jobs/etc.
            geog_growth[col+'_pct_change'] = (round((geog_growth[col+"_"+str(final_year)] / 
                                                     geog_growth[col+"_"+str(initial_summary_year)] - 1) * 100, 2))

            # percent geography's growth of households/jobs/etc. of all regional growth in households/jobs/etc.
            geog_growth[col+'_pct_of_regional_growth'] = round(geog_growth[col+"_growth"] / geog_growth[col+"_growth"].sum() * 100, 2)

            # change in the regional share of households/jobs/etc. in the geography      
            geog_growth[col+"_"+str(initial_summary_year)+"_regional_share"] = (round(geog_growth[col+"_"+str(initial_summary_year)] / 
                                                                                     geog_growth[col+"_"+str(initial_summary_year)].sum(), 2))
            geog_growth[col+"_"+str(final_year)+"_regional_share"] = (round(geog_growth[col+"_"+str(final_year)] / 
                                                                           geog_growth[col+"_"+str(final_year)].sum(), 2))            
            geog_growth[col+'_regional_share_change'] = (geog_growth[col+"_"+str(final_year)+"_regional_share"] - 
                                                         geog_growth[col+"_"+str(initial_summary_year)+"_regional_share"])
    
        geog_growth.fillna(0).to_csv(geosum_output_dir / f"{geography}_summary_growth.csv")
