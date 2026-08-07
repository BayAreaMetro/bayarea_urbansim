from __future__ import print_function

import os
import pathlib
import orca
import pandas as pd

import logging

# Get a logger specific to this module
logger = logging.getLogger(__name__)

@orca.step()
def parcel_summary(run_name, parcels, buildings, households, jobs, year, initial_summary_year, final_year, interim_summary_years):

    if year not in [initial_summary_year, final_year] + interim_summary_years:
        return

    df = parcels.to_frame(["geom_id", "x", "y", 'max_dua', 'built_dua', 'max_far', 'built_far'])
    
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

    # Store initial and final values before calculating growth
    df1[f'residential_units_{initial_summary_year}'] = df1['residential_units'].fillna(0)
    df1[f'residential_units_{final_year}'] = df2['residential_units'].fillna(0)
    df1[f'built_dua_{initial_summary_year}'] = df1['built_dua'].fillna(0)
    df1[f'built_dua_{final_year}'] = df2['built_dua'].fillna(0)
    df1[f'built_far_{initial_summary_year}'] = df1['built_far'].fillna(0)
    df1[f'built_far_{final_year}'] = df2['built_far'].fillna(0)

    for col in df1.columns:
        if col in ["geom_id", "x", "y", 
                   f'residential_units_{initial_summary_year}', f'residential_units_{final_year}',
                   f'built_dua_{initial_summary_year}', f'built_dua_{final_year}',
                   f'built_far_{initial_summary_year}', f'built_far_{final_year}']:
            continue

        # fill na with 0 otherwise it drops the parcel data during subtraction
        df1[col].fillna(0, inplace=True)
        df2[col].fillna(0, inplace=True)

        df1[col] = df2[col] - df1[col]

    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df1.to_csv(coresum_output_dir / f"{run_name}_parcel_growth.csv")
    


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
    df.to_csv(coresum_output_dir / f"{run_name}_building_summary_{year}.csv")


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
             #'urbanized', 
             'manual_nodev', 'total_non_residential_sqft',	'nodev',	
             'built_far', 'max_far', 'built_dua', 'max_dua', 'building_purchase_price_sqft',	
             'building_purchase_price',	'land_cost', 'slr_nodev']]

    df["run_name"] = run_name

    df = df.fillna(0)
    coresum_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    coresum_output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(coresum_output_dir / f"{run_name}_new_buildings_summary.csv")


def build_block_supply(parcel_block, buildings, zoning, feasibility):
    """Rolls parcel-level supply and deliverable capacity up to census blocks.

    Pure (no orca, no file I/O) so it can be unit-tested directly. Produces three
    block-keyed roll-ups side by side: realized new supply (buildings whose
    ``source`` is not ``h5_inputs``), zoned capacity, and profitable residential
    capacity (residential form only, counted where ``max_profit`` is positive).

    Because ``parcel_block`` is the areal crosswalk that maps a parcel to every
    block it overlaps, each parcel quantity is apportioned to blocks by the
    parcel's area share (``parcel_block_share``) before summing, so a parcel that
    straddles a block boundary contributes its share to each block and block totals
    conserve the parcel totals.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns; a parcel appears
            once per overlapping block.
        buildings: DataFrame with ``parcel_id``, ``residential_units``,
            ``deed_restricted_units``, ``non_residential_sqft``, ``job_spaces``,
            and ``source``; base-year stock is filtered out inside this function.
        zoning: DataFrame indexed by ``parcel_id`` with ``zoned_du`` and
            ``zoned_du_underbuild``.
        feasibility: DataFrame indexed by ``parcel_id`` with a ``(form, attribute)``
            MultiIndex on its columns (the ``feasibility_after_policy`` frame).

    Returns:
        A DataFrame indexed by ``block_geoid`` with ``zoned_du``,
        ``zoned_du_underbuild``, ``built_residential_units``,
        ``built_deed_restricted_units``, ``built_non_residential_sqft``,
        ``built_job_spaces``, and ``profitable_residential_units``. Blocks absent
        from a given roll-up are reported as 0.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> buildings = pd.DataFrame({"parcel_id": [1], "residential_units": [5],
        ...     "deed_restricted_units": [0], "non_residential_sqft": [0],
        ...     "job_spaces": [0], "source": ["developer_model"]})
        >>> zoning = pd.DataFrame({"zoned_du": [10], "zoned_du_underbuild": [4]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> cols = pd.MultiIndex.from_tuples(
        ...     [("residential", "total_residential_units"), ("residential", "max_profit")])
        >>> feasibility = pd.DataFrame([[8, 1.0]],
        ...     index=pd.Index([1], name="parcel_id"), columns=cols)
        >>> build_block_supply(parcel_block, buildings, zoning, feasibility).loc["A", "built_residential_units"]
        5.0

    See Also:
        block_supply_summary: the orca step that feeds this helper live model
            tables and the ``feasibility_after_policy`` injectable.
    """
    def _apportion_to_blocks(parcel_values):
        # parcel_values: DataFrame indexed by parcel_id. Broadcast each parcel's
        # values across the blocks it overlaps, weight by parcel_block_share, and
        # sum to block totals.
        merged = parcel_block.join(parcel_values, how="inner")
        value_cols = list(parcel_values.columns)
        weighted = merged[value_cols].multiply(merged["parcel_block_share"], axis=0)
        weighted["block_geoid"] = merged["block_geoid"]
        return weighted.groupby("block_geoid")[value_cols].sum()

    new_buildings = buildings[~buildings.source.isin(["h5_inputs"])]
    parcel_built = new_buildings.groupby("parcel_id")[
        ["residential_units", "deed_restricted_units",
         "non_residential_sqft", "job_spaces"]].sum()
    realized_by_block = _apportion_to_blocks(parcel_built).rename(columns={
        "residential_units": "built_residential_units",
        "deed_restricted_units": "built_deed_restricted_units",
        "non_residential_sqft": "built_non_residential_sqft",
        "job_spaces": "built_job_spaces"})

    zoned_by_block = _apportion_to_blocks(zoning[["zoned_du", "zoned_du_underbuild"]])

    residential_units = feasibility[("residential", "total_residential_units")]
    residential_profit = feasibility[("residential", "max_profit")]
    profitable_units = residential_units.where(residential_profit > 0, 0.0)
    profitable_units = profitable_units.rename("profitable_residential_units").to_frame()
    profitable_by_block = _apportion_to_blocks(profitable_units)

    block_supply = (zoned_by_block
                    .join(realized_by_block, how="outer")
                    .join(profitable_by_block, how="outer")
                    .fillna(0))
    return block_supply


@orca.step()
def block_supply_summary(run_name, buildings, parcels_zoning_calculations, parcels_block,
                         year, initial_summary_year, final_year, interim_summary_years):
    """Writes a census-block roll-up of realized supply, zoned and profitable capacity.

    Additive Phase 1 QAQC step: it reads existing model tables and the
    ``feasibility_after_policy`` injectable, apportions parcel supply and capacity
    to the 2020 census blocks each parcel overlaps via the areal ``parcels_block``
    crosswalk, and writes one CSV per summary year. It changes no model behavior.
    It must run after ``residential_developer`` in the same year so the
    ``feasibility_after_policy`` injectable holds that year's post-policy values.

    Args:
        run_name: Name of the current run, used in the output filename.
        buildings: The orca ``buildings`` table.
        parcels_zoning_calculations: The orca table carrying ``zoned_du`` and
            ``zoned_du_underbuild`` per parcel.
        parcels_block: The areal parcel-to-block crosswalk table (``block_geoid``
            and ``parcel_block_share`` columns; non-unique ``parcel_id`` index).
        year: The current simulation year.
        initial_summary_year: First year at which summaries are written.
        final_year: Final simulation/summary year.
        interim_summary_years: List of intermediate summary years.

    Returns:
        None. Writes ``{run_name}_block_supply_summary_{year}.csv`` to the
        ``core_summaries`` output directory.

    See Also:
        build_block_supply: the pure helper that performs the roll-up arithmetic.
    """
    if year not in [initial_summary_year, final_year] + interim_summary_years:
        return

    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    buildings_df = buildings.to_frame(
        ["parcel_id", "residential_units", "deed_restricted_units",
         "non_residential_sqft", "job_spaces", "source"])
    zoning_df = parcels_zoning_calculations.to_frame(["zoned_du", "zoned_du_underbuild"])
    feasibility = orca.get_injectable("feasibility_after_policy")

    block_supply = build_block_supply(parcel_block, buildings_df, zoning_df, feasibility)

    coresum_output_dir = os.path.join(orca.get_injectable("outputs_dir"), "core_summaries")
    os.makedirs(coresum_output_dir, exist_ok=True)
    # ``parcels_block`` now loads ``block_geoid`` as int64 (numeric alternatives key
    # for the block-choice models). Re-pad it to the 15-digit zero-filled GEOID
    # string on write so this CSV artifact is byte-identical to the string-keyed
    # baseline (California GEOIDs start "06...").
    block_supply.index = block_supply.index.map(lambda geoid: str(geoid).zfill(15))
    block_supply.to_csv(os.path.join(
        coresum_output_dir, "{}_block_supply_summary_{}.csv".format(run_name, year)))


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
    jobs['is_transit_hub'] = (jobs.ec5_cat=="EC5 Target Area").map({True:'job_in_ec5_area',False:'job_not_in_ec5_area'})

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
    zones.to_csv(coresum_output_dir / f"{run_name}_interim_zone_output_{year}.csv")

    extra_hh_cols = ['base_income_quartile',
    'building_id',
    'tenure',
    'unittype',
    'unit_num',
    'unit_id']
    households_df = orca.get_table('households').to_frame()

    households_df[extra_hh_cols].to_csv(coresum_output_dir / f"{run_name}_households_output_{year}.csv")
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


@orca.step()
def account_summary(year,run_name):
    acct_output_dir = pathlib.Path(orca.get_injectable("outputs_dir")) / "core_summaries"
    acct_output_dir.mkdir(parents=True, exist_ok=True)
    
    for acct_name, acct in orca.get_injectable("coffer").items():
        fname = f"{run_name}_acctlog_{acct_name}_{year}.csv"
        acct.to_frame().to_csv(acct_output_dir / fname)