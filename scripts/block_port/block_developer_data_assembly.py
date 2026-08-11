"""Assemble the block residential developer LCM estimation dataset (DV + IVs).

Single data-assembly entry point for the block residential developer
location-choice model. It
builds both halves of the estimation frame and joins them on the 2020 census
block:

- **Dependent variable (DV):** the net 2010->2020 block housing-unit change.
  Sources decennial total-housing-unit counts for the nine Bay Area counties from
  the Census API via `pytidycensus`, caches them under the block sandbox,
  harmonizes the 2010 counts onto 2020 census blocks with the NHGIS 2010->2020
  block crosswalk, and differences them.  This is the pre-LIHTC DV; the
  market-rate residual (net change minus new-construction LIHTC) is produced by a
  later step that subtracts geocoded LIHTC completions.

- **Independent variables (IVs):** the 2010 BAUS base-year parcel covariates
  (accessibility, zoning, and land-use attributes exported for the affordable-
  housing developer model), aggregated from p10 parcels onto 2020 census blocks
  using the areal (polygon-intersection) `parcel_id` -> `block_geoid`
  crosswalk.  Because that crosswalk maps a parcel to every block it overlaps, the
  combine rules apportion by intersection: extensive quantities are apportioned by
  each parcel's area share and summed; intensive accessibility measures are
  averaged weighted by intersection area; zoning-capacity and binary presence
  flags take the block maximum; descriptive labels take the first intersecting
  parcel's value.  Derived ratios and log transforms are recomputed on the
  aggregated block totals, not aggregated.

The primary entry point returns the joined estimation frame in memory and writes
no estimation artifact of its own (the DV CSV is cached as a reusable input); a
downstream estimation script consumes the frame and writes the model-spec yaml in
the shape of the existing location-choice specs.

Usage::

    # First run fetches the decennial counts (needs a Census API key); re-runs
    # read the cached decennial CSVs.
    set CENSUS_API_KEY=YOUR_KEY
    python scripts/block_port/block_developer_data_assembly.py

    # Or, in memory:
    from scripts.block_port.block_developer_data_assembly import (
        build_block_developer_dataset,
    )
    estimation_frame = build_block_developer_dataset(census_api_key="YOUR_KEY")
"""

import os
import pathlib

import numpy as np
import pandas as pd
import pytidycensus

__all__ = [
    # === Dependent variable: decennial net housing-unit change ===
    # Building blocks -- transform data already in memory
    'load_block_crosswalk',
    'allocate_2010_hu_to_2020_blocks',
    # Lower-level loader -- cache-aware decennial fetch
    'fetch_decennial_block_housing_units',
    # DV builder -- fetch, harmonize, difference, cache, return
    'build_block_hu_change',
    # === Independent variables: parcel-aggregated block covariates ===
    # Building blocks -- load a single input into memory
    'load_parcel_covariates',
    'load_parcel_block_crosswalk',
    # Core transform -- parcel covariates -> block covariates
    'aggregate_covariates_to_blocks',
    'build_block_covariates',
    # === Primary entry point -- assemble DV + IVs into the estimation frame ===
    'build_block_developer_dataset',
]

# ---------------------------------------------------------------------------- #
#                                  Constants                                    #
# ---------------------------------------------------------------------------- #

# Bay Area county FIPS codes (3-digit; CA state FIPS = 06).  Mirrors
# scripts/metrics/fetch_2020_blocks.py and the fms-notebook-hub benchmark script.
_BAY_AREA_COUNTY_FIPS = ['001', '013', '041', '055', '075', '081', '085', '095', '097']

# Decennial total-housing-unit variable IDs.
#   2010 SF1 table H1: H001001   (verify against the installed pytidycensus)
#   2020 PL 94-171 table H1: H1_001N
_HU_VAR_2010 = 'H001001'
_HU_VAR_2020 = 'H1_001N'

# Read-only NHGIS 2010->2020 block crosswalk (already on M:), for harmonizing the
# 2010 decennial counts onto the 2020 block spine.
_BLOCK_CROSSWALK_PATH = pathlib.Path(
    r'M:\Crosswalks\Census\nhgis_blk2010_blk2020_06\nhgis_blk2010_blk2020_06.csv'
)

# 2010 BAUS base-year parcel covariates (the same export the affordable-housing
# developer model builds its covariate set from), keyed by `parcel_id`.
_COVARS_PATH = pathlib.Path(
    r'M:\urban_modeling\baus\BAUS Inputs\affordable_housing'
    r'\baus_baseyear_access\parcels_with_access_baseyear.csv'
)

# Areal p10 parcel -> 2020 census-block crosswalk (polygon intersection, read in
# place from M:).  Maps each parcel to every block it overlaps with the parcel's
# area share and intersection area, mirroring the `parcels_block` orca table.
_PARCELS_BLOCK_XWALK_PATH = pathlib.Path(
    r'M:\urban_modeling\urbansim_cloud\projects\combo\parcel_block20_xwalk.csv'
)

# All decennial caches + the DV artifact live in the block sandbox.
_SANDBOX_DIR = pathlib.Path(r'M:\urban_modeling\baus\FoLUMPP2\baus_block_sandbox\data')

# Decennial housing-unit cache locations (written on first run, read thereafter).
_HU_2010_CACHE = _SANDBOX_DIR / 'block_housing_units_2010_bayarea.csv'
_HU_2020_CACHE = _SANDBOX_DIR / 'block_housing_units_2020_bayarea.csv'

# Net 2010->2020 block housing-unit change (the LCM dependent variable): the DV
# builder's output and the estimation frame's DV input.
_DV_PATH = _SANDBOX_DIR / 'block_hu_change_2010_2020_bayarea.csv'

# Extensive quantities -> apportioned by parcel area share and summed to block totals.
_SUM_COLS = [
    'land_value', 'improvement_value_sum', 'total_sqft',
    'total_residential_units', 'total_job_spaces', 'parcel_acres',
]

# Intensive / accessibility measures -> intersection-area-weighted block average.
_WAVG_COLS = [
    'x', 'y',
    'residential_units_500', 'residential_units_1500', 'residential_units_45',
    'office_1500', 'retail_1500', 'industrial_1500',
    'jobs_500', 'jobs_1500', 'jobs_15', 'jobs_45',
    'jobs_fps_45', 'jobs_her_45', 'jobs_fps_15', 'jobs_her_15',
    'ave_income_500', 'ave_income_1500', 'retail_ratio',
    'retail_sqft_3000', 'sum_income_3000', 'sum_income_40',
    'population', 'poor', 'renters', 'sfdu',
    'ave_sqft_per_unit', 'ave_lot_size_per_unit', 'ave_hhsize',
]

# Zoning-allowed-use flags, binary presence flags, and zoning-capacity ceilings
# -> block maximum ("the block can host this if any parcel can").
_MAX_COLS = [
    'HS', 'HT', 'HM', 'MR', 'OF', 'HO', 'SC', 'IL', 'IW', 'IH', 'RS', 'RB', 'MT', 'ME',
    'is_cpad', 'nodev', 'manual_nodev', 'sdem', 'urban_footprint',
    'embarcadero', 'stanford', 'pacheights',
    'max_dua', 'max_far', 'height',
]

# Descriptive labels -> first parcel's value within the block.
_FIRST_COLS = [
    'county', 'juris_name', 'node_id', 'tmnode_id',
    'oldest_building', 'first_building_type', 'zoning_name',
]


# ---------------------------------------------------------------------------- #
#              Dependent variable: decennial net housing-unit change            #
# ---------------------------------------------------------------------------- #

def load_block_crosswalk(crosswalk_path=_BLOCK_CROSSWALK_PATH):
    """Loads the NHGIS 2010->2020 block crosswalk and validates its weights.

    Reads the block-source crosswalk, keeping the 2010/2020 block GEOIDs and the
    single all-characteristics interpolation `weight` (the expected share of a
    source 2010 block's population and housing located in each target 2020
    block).  Validates that no 2010 source block's weights sum to **more** than 1.

    Per-source-block weights sum to at most 1, not exactly 1: blocks with zero
    2010 population and housing carry weight 0 (nothing to allocate), and -- in
    this statewide CA-target file -- blocks straddling a redrawn state line send
    part of their weight to out-of-state 2020 blocks absent from the file, so
    their in-file weights sum to < 1.  Only over-allocation (sum > 1) signals a
    malformed or wrong-direction crosswalk.

    Args:
        crosswalk_path: Path to the NHGIS `blk2010_blk2020` CSV.

    Returns:
        A DataFrame with columns `blk2010ge`, `blk2020ge` (15-char Census
        GEOIDs as strings) and `weight` (float).

    Raises:
        ValueError: If any 2010 source block's weights sum to more than 1 (beyond
            a small floating-point tolerance), indicating a malformed or
            wrong-direction crosswalk.

    Example:
        >>> crosswalk = load_block_crosswalk()
        >>> {'blk2010ge', 'blk2020ge', 'weight'}.issubset(crosswalk.columns)
        True
    """
    crosswalk = pd.read_csv(
        crosswalk_path,
        dtype={'blk2010ge': str, 'blk2020ge': str},
        usecols=['blk2010ge', 'blk2020ge', 'weight'],
    )
    weight_sums = crosswalk.groupby('blk2010ge')['weight'].sum()
    over_allocated = weight_sums[weight_sums > 1.0 + 1e-6]
    if len(over_allocated) > 0:
        raise ValueError(
            'NHGIS block crosswalk weights sum to more than 1 for '
            f'{len(over_allocated)} source 2010 blocks (e.g. '
            f'{over_allocated.head().to_dict()}); check the crosswalk direction.'
        )
    return crosswalk


def allocate_2010_hu_to_2020_blocks(hu_2010, crosswalk):
    """Harmonizes 2010 block housing-unit counts onto 2020 census blocks.

    Applies the NHGIS interpolation weights to distribute each 2010 block's
    housing units across the 2020 blocks that overlap it, then sums to the 2020
    block index.  This change-of-support step puts the 2010 counts on the same
    2020-block geography as the 2020 counts.

    Args:
        hu_2010: A DataFrame with `blk2010ge` and `hu_2010` columns.
        crosswalk: The crosswalk returned by `load_block_crosswalk`.

    Returns:
        A DataFrame indexed by `blk2020ge` with a single `hu_2010` column of
        interpolated 2010 housing units.

    Example:
        >>> harmonized = allocate_2010_hu_to_2020_blocks(hu_2010, crosswalk)
        >>> harmonized.index.name
        'blk2020ge'

    See Also:
        build_block_hu_change: the DV builder that differences this against 2020.
    """
    allocated = crosswalk.merge(hu_2010, on='blk2010ge')
    allocated['hu_2010'] = allocated['hu_2010'] * allocated['weight']
    return allocated.groupby('blk2020ge')[['hu_2010']].sum()


def fetch_decennial_block_housing_units(year, variable, cache_path, census_api_key):
    """Fetches (or loads cached) decennial block housing-unit counts for the Bay Area.

    On the first run this pulls total housing units for every census block in the
    nine Bay Area counties from the Census API via `pytidycensus.get_decennial`
    (one call per county) and writes a CSV cache.  On subsequent runs it reads
    that cache instead of re-downloading.

    Args:
        year: Decennial census year (2010 or 2020).
        variable: The total-housing-unit variable ID for that year's summary file
            (2010 SF1 `H001001`; 2020 PL 94-171 `H1_001N`).
        cache_path: CSV path to read from if present, else write to.
        census_api_key: Census API key (free signup at
            https://api.census.gov/data/key_signup.html).

    Returns:
        A DataFrame with a 15-char block `GEOID` (string, assembled from the
        `state`/`county`/`tract`/`block` components pytidycensus returns,
        because its own `GEOID` field is only tract-level for blocks) and a
        `housing_units` (float) column.

    Example:
        >>> hu = fetch_decennial_block_housing_units(
        ...     2020, 'H1_001N', _HU_2020_CACHE, api_key)
        >>> 'housing_units' in hu.columns
        True
    """
    if cache_path.exists():
        return pd.read_csv(cache_path, dtype={'GEOID': str})

    sumfile = 'sf1' if year == 2010 else 'pl'
    county_frames = []
    for county_fips in _BAY_AREA_COUNTY_FIPS:
        county_data = pytidycensus.get_decennial(
            year=year,
            geography='block',
            state='CA',
            county=county_fips,
            variables=[variable],
            sumfile=sumfile,
            api_key=census_api_key,
        )
        county_frames.append(county_data)

    housing_units = pd.concat(county_frames, ignore_index=True)
    # pytidycensus returns an 11-char (tract-level) GEOID for block geography;
    # rebuild the full 15-char block GEOID from its components.
    housing_units['GEOID'] = (
        housing_units['state'] + housing_units['county']
        + housing_units['tract'] + housing_units['block']
    )
    housing_units = housing_units.rename(columns={variable: 'housing_units'})
    housing_units = housing_units[['GEOID', 'housing_units']]
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    housing_units.to_csv(cache_path, index=False)
    return housing_units


def build_block_hu_change(
    crosswalk_path=_BLOCK_CROSSWALK_PATH,
    hu_2010_cache=_HU_2010_CACHE,
    hu_2020_cache=_HU_2020_CACHE,
    census_api_key=None,
    output_path=_DV_PATH,
):
    """Builds the net 2010->2020 block housing-unit change and caches it to disk.

    Orchestrates the dependent-variable prep: fetches (or loads cached) decennial
    housing-unit counts for both years, harmonizes the 2010 counts onto 2020
    blocks via the NHGIS crosswalk, differences them, and writes a per-2020 block
    net-change CSV.  This net change is the pre-LIHTC dependent variable for the
    block residential developer LCM.

    Args:
        crosswalk_path: Path to the NHGIS block crosswalk.
        hu_2010_cache: Cache CSV path for 2010 block housing units.
        hu_2020_cache: Cache CSV path for 2020 block housing units.
        census_api_key: Census API key.  Required only on the first run, when the
            caches are absent.
        output_path: Destination CSV for the net-change DV.

    Returns:
        A DataFrame indexed by `blk2020ge` with `hu_2010`, `hu_2020` and
        `hu_change` columns.

    Raises:
        ValueError: If a first-time fetch is required but `census_api_key` is
            not supplied, or if the crosswalk weights fail validation.

    Example:
        >>> change = build_block_hu_change(census_api_key='YOUR_KEY')
        >>> 'hu_change' in change.columns
        True

    See Also:
        allocate_2010_hu_to_2020_blocks: the change-of-support building block.
        build_block_developer_dataset: the entry point that joins this DV to the
            block covariates.
    """
    caches_present = hu_2010_cache.exists() and hu_2020_cache.exists()
    if not caches_present and census_api_key is None:
        raise ValueError(
            'census_api_key is required on the first run to fetch decennial '
            'housing units; free signup at '
            'https://api.census.gov/data/key_signup.html'
        )

    hu_2010 = fetch_decennial_block_housing_units(
        2010, _HU_VAR_2010, hu_2010_cache, census_api_key
    ).rename(columns={'GEOID': 'blk2010ge', 'housing_units': 'hu_2010'})

    hu_2020 = (
        fetch_decennial_block_housing_units(
            2020, _HU_VAR_2020, hu_2020_cache, census_api_key
        )
        .rename(columns={'GEOID': 'blk2020ge', 'housing_units': 'hu_2020'})
        .set_index('blk2020ge')
    )

    crosswalk = load_block_crosswalk(crosswalk_path)
    hu_2010_on_2020 = allocate_2010_hu_to_2020_blocks(hu_2010, crosswalk)

    change = hu_2020[['hu_2020']].join(hu_2010_on_2020, how='left')
    change['hu_change'] = change['hu_2020'] - change['hu_2010']

    output_path.parent.mkdir(parents=True, exist_ok=True)
    change.to_csv(output_path)
    print(f'Wrote {len(change):,} block net-change rows -> {output_path}')
    return change


# ---------------------------------------------------------------------------- #
#              Independent variables: parcel-aggregated covariates             #
# ---------------------------------------------------------------------------- #

def load_parcel_covariates(covars_path=_COVARS_PATH):
    """Loads the 2010 BAUS base-year parcel covariate frame.

    Reads the affordable-housing developer model's base-year export, which holds
    the accessibility (pandana walk/drive), zoning, jurisdiction, and land-use
    attributes computed for each 2010 p10 parcel.

    Args:
        covars_path: Path to `parcels_with_access_baseyear.csv`.

    Returns:
        A DataFrame indexed by `parcel_id` with one column per base-year
        covariate.

    Example:
        >>> parcel_covariates = load_parcel_covariates()
        >>> parcel_covariates.index.name
        'parcel_id'
    """
    return pd.read_csv(covars_path, index_col=0)


def load_parcel_block_crosswalk(xwalk_path=_PARCELS_BLOCK_XWALK_PATH):
    """Loads the areal p10 parcel -> 2020 census-block crosswalk.

    Reads the polygon-intersection crosswalk that maps each p10 parcel to every
    2020 census block it overlaps, carrying the share of the parcel in each block
    (`parcel_block_share`) and the intersection area (`intersection_area_sqm`).
    A parcel straddling a block boundary therefore appears on multiple rows.  This
    replaces the earlier centroid (one-block-per-parcel) crosswalk, which dropped
    blocks whose overlapping parcels had centroids in adjacent blocks (mirrors the
    `parcels_block` orca table in `baus/datasources.py`).

    Args:
        xwalk_path: Path to `parcel_block20_xwalk.csv`.

    Returns:
        A DataFrame indexed by `parcel_id` (non-unique) with `block_geoid` (the
        15-digit 2020 GEOID as a string), `parcel_block_share` and
        `intersection_area_sqm` columns.

    Example:
        >>> parcel_block = load_parcel_block_crosswalk()
        >>> {'block_geoid', 'parcel_block_share'}.issubset(parcel_block.columns)
        True
    """
    parcel_block = pd.read_csv(
        xwalk_path,
        usecols=['parcel_id', 'block_id', 'parcel_block_share', 'intersection_area_sqm'],
        dtype={'parcel_id': np.int64, 'block_id': str},
    )
    return parcel_block.rename(columns={'block_id': 'block_geoid'}).set_index('parcel_id')


def aggregate_covariates_to_blocks(parcel_covariates, parcel_block):
    """Aggregates parcel covariates onto 2020 census blocks.

    Joins the parcel covariates to the areal parcel->block crosswalk and combines
    each column by its kind: extensive quantities are apportioned to blocks by the
    parcel's area share and summed, intensive accessibility/neighborhood measures
    are averaged weighted by intersection area, zoning-capacity and binary flags
    take the block maximum over intersecting parcels, and descriptive labels take
    the first intersecting parcel's value.  Derived ratios and log transforms are
    recomputed on the aggregated block totals so they stay internally consistent.

    Args:
        parcel_covariates: Parcel-indexed covariates from
            `load_parcel_covariates`.
        parcel_block: Areal parcel->block crosswalk from
            `load_parcel_block_crosswalk` (`block_geoid`,
            `parcel_block_share`, `intersection_area_sqm`; non-unique
            `parcel_id` index).

    Returns:
        A DataFrame indexed by `block_geoid` with the summed, weighted-averaged,
        maxed, and first-valued covariates plus the recomputed derived columns
        (`built_dua`, `zoned_du_build_ratio`, `total_assessed_value`, and
        the `log_*` transforms).

    Example:
        >>> block_covariates = aggregate_covariates_to_blocks(
        ...     parcel_covariates, parcel_block)
        >>> block_covariates.index.name
        'block_geoid'

    See Also:
        build_block_covariates: the entry point that wires the loaders into this.
    """
    covariates = parcel_block.join(parcel_covariates, how='inner')
    grouped = covariates.groupby('block_geoid')

    apportioned = covariates[_SUM_COLS].multiply(covariates['parcel_block_share'], axis=0)
    apportioned['block_geoid'] = covariates['block_geoid']
    block_covariates = apportioned.groupby('block_geoid')[_SUM_COLS].sum()

    intersection_weight = (
        covariates['intersection_area_sqm']
        / grouped['intersection_area_sqm'].transform('sum')
    )
    weighted = covariates[_WAVG_COLS].multiply(intersection_weight, axis=0)
    weighted['block_geoid'] = covariates['block_geoid']
    block_covariates = block_covariates.join(weighted.groupby('block_geoid').sum())

    block_covariates = block_covariates.join(grouped[_MAX_COLS].max())
    block_covariates = block_covariates.join(grouped[_FIRST_COLS].first())

    block_covariates['built_dua'] = (
        block_covariates['total_residential_units'] / block_covariates['parcel_acres']
    )
    # zoned_du_build_ratio is undefined where a block has zero zoned capacity
    # (max_dua == 0): 0/0 for undeveloped blocks and built/0 for blocks built out
    # under superseded or as-of-right zoning. Both cases are recoded to 0.
    block_covariates['zoned_du_build_ratio'] = np.where(
        block_covariates['max_dua'] == 0,
        0,
        block_covariates['built_dua'] / block_covariates['max_dua'].replace(0, np.nan),
    )
    block_covariates['total_assessed_value'] = (
        block_covariates['land_value'] + block_covariates['improvement_value_sum']
    )
    block_covariates['log_land_value'] = np.log1p(block_covariates['land_value'])
    block_covariates['log_parcel_acres'] = np.log1p(block_covariates['parcel_acres'])
    block_covariates['log_retail_sqft_3000'] = np.log1p(block_covariates['retail_sqft_3000'])
    block_covariates['log_total_assessed_value'] = np.log1p(
        block_covariates['total_assessed_value']
    )
    return block_covariates


def build_block_covariates(covars_path=_COVARS_PATH, xwalk_path=_PARCELS_BLOCK_XWALK_PATH):
    """Builds the 2020-block covariate (IV) frame from base-year parcels.

    Loads the base-year parcel covariates and the areal parcel->block crosswalk,
    then aggregates the covariates onto 2020 blocks.  This is the independent-
    variable half of the estimation frame; the dependent variable is joined by
    `build_block_developer_dataset`.

    Args:
        covars_path: Path to the base-year parcel covariate CSV.
        xwalk_path: Path to the areal parcel -> 2020-block crosswalk CSV.

    Returns:
        A DataFrame indexed by `block_geoid` with the aggregated block
        covariates.

    Example:
        >>> block_covariates = build_block_covariates()
        >>> block_covariates.index.name
        'block_geoid'

    See Also:
        aggregate_covariates_to_blocks: the parcel->block combine step.
        build_block_developer_dataset: the entry point that joins the DV onto this.
    """
    parcel_covariates = load_parcel_covariates(covars_path)
    parcel_block = load_parcel_block_crosswalk(xwalk_path)
    return aggregate_covariates_to_blocks(parcel_covariates, parcel_block)


# ---------------------------------------------------------------------------- #
#                 Primary entry point: DV + IVs estimation frame               #
# ---------------------------------------------------------------------------- #

def build_block_developer_dataset(
    covars_path=_COVARS_PATH,
    parcels_block_xwalk_path=_PARCELS_BLOCK_XWALK_PATH,
    block_crosswalk_path=_BLOCK_CROSSWALK_PATH,
    hu_2010_cache=_HU_2010_CACHE,
    hu_2020_cache=_HU_2020_CACHE,
    dv_path=_DV_PATH,
    census_api_key=None,
):
    """Assembles the block residential developer LCM estimation frame.

    Builds the dependent variable (net 2010->2020 block housing-unit change) and
    the independent variables (base-year parcel covariates aggregated to 2020
    blocks), then inner-joins them on `block_geoid`.  The inner join keeps only
    blocks that both contain p10 parcels and appear in the decennial DV.  The DV
    is computed in memory (and cached to `dv_path` as a reusable input); the
    joined estimation frame is returned in memory and written nowhere -- a
    downstream estimation script consumes it and writes the model-spec yaml.

    Args:
        covars_path: Path to the base-year parcel covariate CSV.
        parcels_block_xwalk_path: Path to the areal parcel -> 2020-block crosswalk.
        block_crosswalk_path: Path to the NHGIS 2010->2020 block crosswalk.
        hu_2010_cache: Cache CSV path for 2010 block housing units.
        hu_2020_cache: Cache CSV path for 2020 block housing units.
        dv_path: Destination CSV for the cached net-change DV.
        census_api_key: Census API key.  Required only on the first run, when the
            decennial caches are absent.

    Returns:
        A DataFrame indexed by `block_geoid` with the aggregated block covariates
        and the `hu_2010`, `hu_2020` and `hu_change` columns.

    Raises:
        ValueError: If a first-time decennial fetch is required but
            `census_api_key` is not supplied, or if the block crosswalk weights
            fail validation.

    Example:
        >>> estimation_frame = build_block_developer_dataset(census_api_key='YOUR_KEY')
        >>> 'hu_change' in estimation_frame.columns
        True

    See Also:
        build_block_hu_change: builds the dependent variable.
        build_block_covariates: builds the independent-variable block covariates.
    """
    housing_unit_change = build_block_hu_change(
        crosswalk_path=block_crosswalk_path,
        hu_2010_cache=hu_2010_cache,
        hu_2020_cache=hu_2020_cache,
        census_api_key=census_api_key,
        output_path=dv_path,
    ).rename_axis('block_geoid')

    block_covariates = build_block_covariates(
        covars_path=covars_path,
        xwalk_path=parcels_block_xwalk_path,
    )

    estimation_frame = block_covariates.join(
        housing_unit_change[['hu_2010', 'hu_2020', 'hu_change']], how='inner'
    )
    return estimation_frame


if __name__ == '__main__':
    frame = build_block_developer_dataset(census_api_key=os.getenv('CENSUS_API_KEY'))
    developed_blocks = (frame['hu_change'] > 0).sum()
    print(f'Block estimation frame: {frame.shape[0]:,} blocks x {frame.shape[1]} columns')
    print(
        f"  hu_change total: {frame['hu_change'].sum():,.0f}; "
        f'developed blocks (hu_change > 0): {developed_blocks:,}'
    )
