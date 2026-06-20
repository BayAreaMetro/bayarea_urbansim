from __future__ import print_function

import pathlib
import orca
import pandas as pd

import logging

# Get a logger specific to this module
logger = logging.getLogger(__name__)

@orca.step()
def disaggregate_output(parcels, buildings, residential_units, households, jobs, static_parcels,
                        year, initial_summary_year, final_year, interim_summary_years):
    """
    This outputs disaggregate tables at the end of specified simulation years.
    The disaggregate tables output are:
    * parcel_table_{year}.csv
    * building_table_{year}.csv
    * residential_units_{year}.csv
    * household_table_{year}.csv
    * job_table_{year}.csv
    * static_parcels_{year}.csv
    """
    if year not in [initial_summary_year, final_year] + interim_summary_years:
        return

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)

    df = parcels.to_frame(["geom_id", "x", "y", 'max_dua', 'built_dua', 'max_far', 'built_far','parcel_softsite'])
    
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

    df.to_csv(coresum_output_dir / f"parcel_table_{year}.csv")

    ####### disaggregate building output
    df = orca.merge_tables('buildings',
        [parcels, buildings],
        columns=['parcel_id', 'year_built', 'building_type', 'residential_units', 'unit_price', 
                 'non_residential_sqft', 'deed_restricted_units', 'inclusionary_units',
                 'preserved_units', 'subsidized_units', 'job_spaces', 'vacant_job_spaces', 'source'])

    df = df.fillna(0)
    df.to_csv(coresum_output_dir / f"building_table_{year}.csv")

    ####### disaggregate residential_units output
    resunits_df = residential_units.to_frame(columns=[
        'unit_residential_price','unit_residential_rent',
        'num_units','building_id','unit_num',
        'deed_restricted','tenure','vacant_units'])
    resunits_df.index.rename('unit_id', inplace=True)
    resunits_df = resunits_df.reset_index()

    resunits_df.to_csv(coresum_output_dir / f"residential_units_table_{year}.csv", index=False)

    ####### disaggregate household output
    households_df = households.to_frame(columns=[
        'unit_id','unit_num','building_id','persons',
        'income','base_income_quartile','base_income_octile','tenure',
        'move_in_year'])
    households_df.index.rename('household_id', inplace=True)
    households_df = households_df.reset_index()

    households_df.to_csv(coresum_output_dir / f"household_table_{year}.csv", index=False)

    ####### disaggregate jobs output
    jobs_df = jobs.to_frame(columns=['building_id','sector_id','empsix', 'move_in_year'])
    jobs_df.index.rename('job_id', inplace=True)
    jobs_df = jobs_df.reset_index()
    jobs_df.to_csv(coresum_output_dir / f"job_table_{year}.csv", index=False)

    ####### static parcels
    # this is either a list or an ndarray depending on when it's called
    print("static_parcels type={} len={}".format(
        type(static_parcels), len(static_parcels)
    ))
    df = pd.DataFrame(data=static_parcels, columns=["parcel_id"])
    df.to_csv(coresum_output_dir / f"static_parcels_{year}.csv", index=False)

@orca.step()
def parcel_growth_summary(year, run_name, initial_summary_year, final_year):
    
    if year != final_year:
        return

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    initial_parcel_file = coresum_output_dir / f"parcel_table_{initial_summary_year}.csv"
    print(f"initial_parcel_file resolve:{initial_parcel_file.resolve()} exists:{initial_parcel_file.exists()}")

    df1 = pd.read_csv(initial_parcel_file, index_col="parcel_id")
    df2 = pd.read_csv(coresum_output_dir / f"parcel_table_{final_year}.csv",
                      index_col="parcel_id")

    for col in df1.columns:
        if col in ["geom_id", "x", "y","parcel_softsite"]:
            continue

        # fill na with 0 otherwise it drops the parcel data during subtraction
        df1[col].fillna(0, inplace=True)
        df2[col].fillna(0, inplace=True)

        df1[col] = df2[col] - df1[col]

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df1.to_csv(coresum_output_dir / f"parcel_growth.csv")

@orca.step()
def new_buildings_summary(run_name, parcels, parcels_zoning_calculations, buildings, year, final_year):

    if year != final_year:
        return


    pcl_cols = ['parcel_acres', 'x', 'sdem', 'acres', 'max_dua', 'max_far',  'building_purchase_price_sqft',
            'total_residential_units', 'y', 'total_job_spaces', 'land_cost', 'nodev', #'urbanized',
             'manual_nodev', 'land_value', 'built_far', 'building_purchase_price', 'total_non_residential_sqft', 'built_dua',
            'slr_nodev']
    
    pcl_zon_cols = ['zoned_far_underbuild', 'zoned_du_underbuild', 'zoned_du', 'zoned_far',
                'zoned_du_build_ratio', 'zoned_far_build_ratio']
    
    parcels_zoning_df = (parcels.to_frame(columns=pcl_cols)
           .join(parcels_zoning_calculations.to_frame(columns=pcl_zon_cols),
                 lsuffix='parcels'))

    # Get buildings frame
    bldg_cols = ['parcel_id', 'source', 'vacant_residential_units', 'unit_price', 'inclusionary_units',
                'year_built', 'preserved_units', 'residential_units', 'building_sqft', 'vacant_res_units',
                'building_type', 'vacant_job_spaces', 'deed_restricted_units', 'non_residential_sqft',
                'subsidized_units', 'price_per_sqft', 'residential_price']
    
    buildings_df = buildings.to_frame(columns=bldg_cols)
    
    # just keep new, simulation period records
    buildings_df = buildings_df.query('source!="h5_inputs"')
    
    building_types = (buildings_df
                  .groupby('parcel_id')
                  .building_type.apply(lambda x: '-'.join(list(set(x)))))

    df = buildings_df.merge(parcels_zoning_df, left_on="parcel_id", right_index=True,how='inner')
    
    # add building types for each parcel_id
    df['building_types'] = building_types
    #df = df[~df.source.isin(["h5_inputs"])] 

    pcl_cols.append('building_types')

    df = df[bldg_cols + pcl_cols + pcl_zon_cols]

    df["run_name"] = run_name

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"new_buildings_summary.csv")


# 
# Shared helpers for geographic (TAZ / census-tract) interim summaries
# 
def _compute_interim_geo_summary(geo_col, geo_index,
                                  buildings_df, residential_units_df,
                                  parcels_df, households_df, jobs_df):
    """Core aggregation logic shared by interim_zone_output (TAZ) and
    interim_tract_output (census tract).

    All DataFrames must already carry a column named *geo_col* so that
    groupby operations work uniformly for either geography.

    Parameters
    ----------
    geo_col : str
        Column to group by – ``'zone_id'`` for TAZ, ``'census_tract'`` for
        census tracts.
    geo_index : pandas.Index
        Ordered unique values of the geography used as the row index of the
        returned DataFrame.
    buildings_df : pd.DataFrame
        Buildings with *geo_col* and standard building columns, including
        ``building_sqft`` and ``residential_units`` (count) which are used to
        derive the average square footage per dwelling unit.
    residential_units_df : pd.DataFrame
        Residential units with *geo_col*, ``building_id``,
        ``unit_residential_price`` (per sqft), and
        ``unit_residential_rent`` (per sqft).
    parcels_df : pd.DataFrame
        Parcels with *geo_col* and zoning-capacity columns already joined in
        (``zoned_du``, ``zoned_du_underbuild``, ``zoned_far``,
        ``zoned_far_underbuild``).
    households_df : pd.DataFrame
        Households with *geo_col*.
    jobs_df : pd.DataFrame
        Jobs with *geo_col* and ``ec5_cat``.

    Returns
    -------
    pd.DataFrame
        Summary rows indexed by *geo_col* values.

    Notes
    -----
    Residential price / rent variables
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ``residential_price`` and ``residential_rent`` are the median (0.5
    quantile) of the per-**square-foot** hedonic prices stored in
    ``unit_residential_price`` / ``unit_residential_rent`` on the
    residential-units table.

    ``residential_price_per_unit`` and ``residential_rent_per_unit`` are the
    corresponding per-**dwelling-unit** totals, obtained by multiplying each
    unit's per-sqft price by the average unit size of its parent building
    (``building_sqft / residential_units``).  These give a sense of the
    absolute cost of a representative dwelling rather than a density-adjusted
    price signal.
    """
    geo_df = pd.DataFrame(index=geo_index)

    # Jobs by transit-hub status 
    jobs_df = jobs_df.copy()
    jobs_df['is_transit_hub'] = (jobs_df.ec5_cat == "Transit_Hub").map(
        {True: 'job_in_transit_hub', False: 'job_not_in_transit_hub'})
    jobs_by_ec5 = (
        jobs_df.groupby([geo_col, 'is_transit_hub'])
               .size()
               .unstack(1)
               .fillna(0)
               .astype(int)
    )
    geo_df = geo_df.merge(jobs_by_ec5, how='left', left_index=True, right_index=True)

    # Building stock 
    geo_df['non_residential_sqft'] = (
        buildings_df.groupby(geo_col).non_residential_sqft.sum())
    geo_df['non_residential_sqft_office'] = (
        buildings_df.query('building_type=="OF"')
                    .groupby(geo_col).non_residential_sqft.sum())
    geo_df['job_spaces'] = (
        buildings_df.groupby(geo_col).job_spaces.sum())
    geo_df['job_spaces_office'] = (
        buildings_df.query('building_type=="OF"')
                    .groupby(geo_col).job_spaces.sum())
    geo_df['residential_units'] = (
        buildings_df.groupby(geo_col).residential_units.sum())
    geo_df['deed_restricted_units'] = (
        buildings_df.groupby(geo_col).deed_restricted_units.sum())
    geo_df['preserved_units'] = (
        buildings_df.groupby(geo_col).preserved_units.sum())
    geo_df['inclusionary_units'] = (
        buildings_df.groupby(geo_col).inclusionary_units.sum())
    geo_df['subsidized_units'] = (
        buildings_df.groupby(geo_col).subsidized_units.sum())

    # Zoning capacity 
    geo_df['zoned_du'] = parcels_df.groupby(geo_col).zoned_du.sum()
    geo_df['zoned_du_underbuild'] = (
        parcels_df.groupby(geo_col).zoned_du_underbuild.sum())
    geo_df['zoned_du_build_ratio'] = (
        geo_df.residential_units / geo_df.zoned_du)
    geo_df['zoned_far'] = parcels_df.groupby(geo_col).zoned_far.sum()
    geo_df['zoned_far_underbuild'] = (
        parcels_df.groupby(geo_col).zoned_far_underbuild.sum())
    geo_df['zoned_far_build_ratio'] = (
        geo_df.non_residential_sqft / geo_df.zoned_far)

    # Vacancy 
    tothh = (households_df[geo_col].value_counts()
                                    .reindex(geo_df.index)
                                    .fillna(0))
    geo_df['residential_vacancy'] = (
        1.0 - tothh / geo_df.residential_units.replace(0, 1))

    totjobs = (jobs_df[geo_col].value_counts()
                               .reindex(geo_df.index)
                               .fillna(0))
    geo_df['non_residential_vacancy'] = (
        1.0 - totjobs / geo_df.job_spaces.replace(0, 1))

    # Office vacancy: rate based on spaces (not sqft), consistent with
    # non_residential_vacancy above; modest loss of precision vs sqft-based.
    geo_df['non_residential_vacancy_office'] = (
        buildings_df.query('building_type=="OF"')
                    .groupby(geo_col)
                    .apply(lambda x: (x['vacant_job_spaces'].sum().clip(0) /
                                      x['job_spaces'].sum().clip(1)))
    )

    # Prices / rents 
    # Per-sqft hedonic prices (median across residential units in each zone).
    geo_df['residential_price'] = (
        residential_units_df.groupby(geo_col).unit_residential_price.quantile())
    geo_df['residential_rent'] = (
        residential_units_df.groupby(geo_col).unit_residential_rent.quantile())
    geo_df['non_residential_rent'] = (
        buildings_df.groupby(geo_col).non_residential_rent.quantile())

    # Per-unit total prices: per-sqft price × average unit size in the
    # building (building_sqft / residential_units count).  This gives an
    # absolute dollar value for a representative dwelling unit.
    if ('building_sqft' in buildings_df.columns and
            'residential_units' in buildings_df.columns):
        bldg_sqft_per_unit = (
            buildings_df['building_sqft'] /
            buildings_df['residential_units'].clip(lower=1))
        res = residential_units_df.copy()
        res['sqft_per_unit'] = res['building_id'].map(bldg_sqft_per_unit)
        res['price_per_unit'] = res['unit_residential_price'] * res['sqft_per_unit']
        res['rent_per_unit']  = res['unit_residential_rent']  * res['sqft_per_unit']
        geo_df['residential_price_per_unit'] = (
            res.groupby(geo_col).price_per_unit.quantile())
        geo_df['residential_rent_per_unit'] = (
            res.groupby(geo_col).rent_per_unit.quantile())
    else:
        logger.warning(
            "_compute_interim_geo_summary: 'building_sqft' or "
            "'residential_units' not found in buildings_df; "
            "residential_price_per_unit and residential_rent_per_unit "
            "will not be computed.")

    return geo_df



def _save_interim_geo_output(geo_df, year, final_year, coresum_output_dir,
                              output_stem, orca_table_name):
    """Write per-year CSV and maintain the all-years accumulated orca table.

    At *final_year* also writes ``{output_stem}_allyears.csv``.

    Parameters
    ----------
    geo_df : pd.DataFrame
        Summary DataFrame for this year (index = geography IDs).
    year : int
        Current simulation year.
    final_year : int
        Final simulation year (triggers the all-years write).
    coresum_output_dir : pathlib.Path
        Output directory (must already exist).
    output_stem : str
        Base name for output files, e.g. ``'interim_zone_output'`` or
        ``'interim_tract_output'``.
    orca_table_name : str
        Name of the orca table that accumulates all-years data.
    """
    geo_df.to_csv(coresum_output_dir / f"{output_stem}_{year}.csv")

    # Append this year's columns (suffixed with year) to the all-years table
    geo_df_yr = geo_df.add_suffix("_" + str(year))
    try:
        all_years = orca.get_table(orca_table_name).to_frame()
    except KeyError:
        all_years = pd.DataFrame(index=geo_df_yr.index)
    all_years = all_years.merge(geo_df_yr, left_index=True, right_index=True)
    orca.add_table(orca_table_name, all_years)

    if year == final_year:
        all_years.to_csv(coresum_output_dir / f"{output_stem}_allyears.csv")


# 
# TAZ-level interim summary (existing step, refactored to use helpers)
# 

@orca.step()
def interim_zone_output(run_name, households, buildings, residential_units, parcels, jobs, zones, year,
                        parcels_zoning_calculations, initial_summary_year, final_year):
    """Aggregate simulation outputs to TAZ (zone) level each simulation year.

    Outputs per year:
      * core_summaries/interim_zone_output_{year}.csv

    Outputs at final_year:
      * core_summaries/interim_zone_output_allyears.csv
    """
    # TODO: currently TAZ, do we want this to be MAZ?
    geo_col = 'zone_id'
    geo_index = pd.Index(zones.index, name=geo_col)

    # Save zone_id as zone_id_x on parcels before merge_tables; orca may
    # clobber zone_id if another broadcast source provides a conflicting value.
    parcels_df = parcels.to_frame()
    parcels_df["zone_id_x"] = parcels_df.zone_id
    orca.add_table('parcels', parcels_df)
    parcels = orca.get_table("parcels")

    households_df = orca.merge_tables(
        'households', [parcels, buildings, households],
        columns=['zone_id', 'zone_id_x', 'base_income_quartile'])
    households_df[geo_col] = households_df.zone_id_x

    jobs_df = orca.merge_tables(
        'jobs', [parcels, buildings, jobs],
        columns=['zone_id', 'zone_id_x', 'empsix', 'ec5_cat'])
    jobs_df[geo_col] = jobs_df.zone_id_x

    parcels_df = parcels.to_frame()
    parcels_df = parcels_df.join(parcels_zoning_calculations.to_frame(), lsuffix='parcels')

    buildings_df = buildings.to_frame()
    residential_units_df = residential_units.to_frame()
    # Explicitly set zone_id on residential_units via building_id (defensive;
    # zone_id may already be present as a computed orca column).
    residential_units_df[geo_col] = (
        residential_units_df['building_id'].map(buildings_df[geo_col]))

    geo_df = _compute_interim_geo_summary(
        geo_col, geo_index,
        buildings_df, residential_units_df,
        parcels_df, households_df, jobs_df)

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)

    _save_interim_geo_output(
        geo_df, year, final_year, coresum_output_dir,
        output_stem="interim_zone_output",
        orca_table_name="interim_zone_output_all")



# Census-tract-level interim summary
@orca.step()
def interim_tract_output(run_name, households, buildings, residential_units, parcels, jobs, year,
                          parcels_zoning_calculations, initial_summary_year, final_year,
                          parcel_tract_crosswalk):
    """Aggregate simulation outputs to census-tract level each simulation year.

    Analogous to ``interim_zone_output`` but grouped by census tract, using the
    parcel-to-tract crosswalk provided by ``parcel_tract_crosswalk()``
    (``parcel_tract_xwalk.csv``, indexed by ``parcel_id`` with a
    ``GEOID10`` column).

    Census tract is propagated down the spatial hierarchy as follows::

        parcel_tract_crosswalk  (parcel_id → GEOID10)
            └- parcels      (add GEOID10 via index alignment)
                └- buildings    (map parcel_id → GEOID10)
                    └- residential_units  (map building_id → GEOID10)
        households / jobs: merged with parcel_id then mapped to GEOID10

    Outputs per year:
      * core_summaries/interim_tract_output_{year}.csv

    Outputs at final_year:
      * core_summaries/interim_tract_output_allyears.csv
    """
    geo_col = 'GEOID10'

    # Build tract index from the parcel→tract crosswalk 
    # parcel_tract_crosswalk is indexed by parcel_id; GEOID10 is the tract GEOID.
    parcel_tract_crosswalk_df = parcel_tract_crosswalk.to_frame(columns=['parcel_id','GEOID10'])
    geo_index = pd.Index(
        sorted(parcel_tract_crosswalk_df['GEOID10'].dropna().unique()),
        name=geo_col)

    # Parcels: add GEOID10 and join zoning calculations 
    # Both parcels and parcel_tract_crosswalk are indexed by parcel_id, so alignment
    # via pandas index assignment works directly.
    parcels_df = parcels.to_frame()
    parcels_df[geo_col] = parcel_tract_crosswalk_df.set_index('parcel_id')['GEOID10']
    parcels_df = parcels_df.join(parcels_zoning_calculations.to_frame(), lsuffix='parcels')

    # Buildings: propagate GEOID10 via parcel_id 
    buildings_df = buildings.to_frame()
    buildings_df[geo_col] = buildings_df['parcel_id'].map(parcels_df[geo_col])

    # Residential units: propagate GEOID10 via building_id 
    residential_units_df = residential_units.to_frame()
    residential_units_df[geo_col] = (
        residential_units_df['building_id'].map(buildings_df[geo_col]))

    # Households: merge to parcel level, then map to GEOID10 
    households_df = orca.merge_tables(
        'households', [parcels, buildings, households],
        columns=['parcel_id', 'base_income_quartile'])
    households_df[geo_col] = households_df['parcel_id'].map(parcels_df[geo_col])

    # Jobs: merge to parcel level (picks up ec5_cat), map to GEOID10 
    jobs_df = orca.merge_tables(
        'jobs', [parcels, buildings, jobs],
        columns=['parcel_id', 'empsix', 'ec5_cat'])
    jobs_df[geo_col] = jobs_df['parcel_id'].map(parcels_df[geo_col])

    # Compute summary 
    geo_df = _compute_interim_geo_summary(
        geo_col, geo_index,
        buildings_df, residential_units_df,
        parcels_df, households_df, jobs_df)

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)

    _save_interim_geo_output(
        geo_df, year, final_year, coresum_output_dir,
        output_stem="interim_tract_output",
        orca_table_name="interim_tract_output_all")
