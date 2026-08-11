"""Block-level residential developer.

Additive, opt-in alternative to the parcel-level `residential_developer` step
(`baus/models.py`). When `run_setup["developer_geography"]` is `"block"`,
`baus.py` swaps this step in for `residential_developer` in the annual model
list; when it is `"parcel"` (the default), this module is never invoked and
behavior is unchanged.

The step allocates the region's residential development target to census blocks
with a **block-native** rule and then renders those block unit counts down to
parcels so the rest of BAUS (hedonics, HLCM, tenure, summaries) runs unchanged:

1. **Regional target** -- `Developer.compute_units_to_build` from live household
   and unit counts, identical to the parcel path.
2. **Deliverable capacity per block** -- `build_block_supply`'s roll-up of this
   year's post-policy feasibility (`profitable_residential_units`). This is the
   hard cap: units only go to blocks with profitable pro-forma capacity.
3. **Development-probability surface** -- the fitted block LCM
   (`configs/developer/block_residential_developer.yaml`) scored over a static
   block covariate table.  Higher-probability blocks are more likely to be drawn.
4. **Sequential stochastic weighted draw** -- blocks are drawn without replacement
   with probability proportional to their LCM weight, each allocated
   `min(deliverable_capacity, remaining_need)` units, until the regional target
   is met or eligible capacity is exhausted (`allocate_units_to_blocks`).
5. **Rendering bridge** -- within each allocated block, profitable feasibility
   parcels are picked by profit (`subsidies.profit_to_prob_func`, unchanged from
   the parcel path) via `Developer.pick` until the block's allocation is met; the
   picked parcels are fabricated into building rows and merged into the `buildings`
   table exactly as `run_developer` does.

Simplifications relative to the parcel path:

- **Static covariates.** The development-probability surface is scored from a
  pre-assembled base-year block covariate table, so the surface is fixed across
  simulation years.
- **Jurisdiction limits via dominant-block mapping.** Per-jurisdiction rollover
  targets are computed exactly as the parcel path (`limits_settings`), then each
  target is allocated only across the blocks whose dominant jurisdiction matches
  (blocks are assigned a dominant jurisdiction by summed `parcel_block_share`).
  The horizon-overshoot trim the parcel path applies to its last building is
  omitted.
- **Areal capacity vs. dominant-block rendering.** Capacity is apportioned areally
  by `build_block_supply` while rendering assigns each parcel to its single
  dominant block, so a block occasionally cannot fully deliver its areal-apportioned
  allocation; `Developer.pick` self-limits and the regional total lands at or just
  below target.
"""

import os
import pathlib

import numpy as np
import pandas as pd
import orca
import yaml
from urbansim.developer.developer import Developer

from baus import subsidies
from baus.utils import add_buildings
from baus.summaries.core_summaries import build_block_supply

__all__ = [
    # Building blocks -- pure, unit-testable functions
    'score_blocks',
    'allocate_units_to_blocks',
    'allocate_units_to_blocks_by_jurisdiction',
    # Primary entry point -- the orca step wired into the annual model list
    'block_residential_developer',
]

# Repo-relative path to the fitted block-LCM spec (raw-coefficient space).
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_SPEC_YAML_PATH = _REPO_ROOT / 'configs' / 'developer' / 'block_residential_developer.yaml'

# Pre-assembled static block covariate table (the 7 model covariates keyed by the
# 15-digit 2020 census-block GEOID), produced offline by
# `scripts.block_port.block_developer_data_assembly.build_block_covariates`.
_BLOCK_COVARIATES_PATH = pathlib.Path(
    r'M:\urban_modeling\baus\FoLUMPP2\baus_block_sandbox\data\block_developer_covariates.csv'
)


# ---------------------------------------------------------------------------- #
#                        Building blocks (pure functions)                       #
# ---------------------------------------------------------------------------- #

def score_blocks(block_covariates, spec):
    """Scores the block development-probability surface from a fitted LCM spec.

    Evaluates the spec's `model_expression` (a sum of plain covariate column
    names) against the raw-scale coefficients in `fit_parameters['Coefficient']`
    to form the linear predictor, then applies the logistic link to produce a
    development probability per block.  The coefficients are in raw covariate space
    (no standardization -- see `scripts/block_port/block_developer_train.py`), so
    this is a direct expression eval with no scaler.

    Args:
        block_covariates: DataFrame indexed by `block_geoid` with one column per
            model covariate named in the spec's `model_expression`.
        spec: The parsed block-LCM spec dict (from the fitted yaml), carrying
            `model_expression` and a `fit_parameters['Coefficient']` mapping
            keyed by `Intercept` plus each covariate name.

    Returns:
        A Series indexed by `block_geoid` of development probabilities in
        `(0, 1)`, aligned to `block_covariates.index`.

    Example:
        >>> import pandas as pd
        >>> covariates = pd.DataFrame({'x': [0.0, 1.0]},
        ...     index=pd.Index(['A', 'B'], name='block_geoid'))
        >>> spec = {'model_expression': 'x',
        ...     'fit_parameters': {'Coefficient': {'Intercept': 0.0, 'x': 0.0}}}
        >>> score_blocks(covariates, spec).round(3).tolist()
        [0.5, 0.5]

    See Also:
        allocate_units_to_blocks: consumes these weights to draw and cap blocks.
        block_residential_developer: loads the spec + covariates and calls this.
    """
    coefficients = spec['fit_parameters']['Coefficient']
    terms = [term.strip() for term in spec['model_expression'].split('+')]

    linear_predictor = pd.Series(
        float(coefficients['Intercept']), index=block_covariates.index)
    for term in terms:
        linear_predictor = linear_predictor + float(coefficients[term]) * block_covariates[term]

    return 1.0 / (1.0 + np.exp(-linear_predictor))


def allocate_units_to_blocks(block_weights, block_capacity, regional_target_units,
                             random_state=None):
    """Allocates the regional unit target to blocks by sequential stochastic draw.

    Blocks are drawn **without replacement** with probability proportional to
    their LCM development weight,
    and each drawn block receives `min(deliverable_capacity, remaining_need)`
    units, until the regional target is met or eligible capacity is exhausted.  A
    block is eligible only if it has at least one whole unit of deliverable
    capacity and a positive weight -- there is no fallback to zoned capacity, so
    units only land where a profitable pro-forma exists.  Because allocation is
    capped at each block's own capacity and the block is then removed from the
    pool, no block is ever over-filled and no overflow-redistribution pass is
    needed.

    The weighted draw uses the Efraimidis-Spirakis one-pass method: drawing keys
    `u ** (1 / weight)` and taking blocks in descending key order is equivalent
    to repeatedly sampling one block proportional to its remaining weight, but runs
    in `O(n log n)` instead of a per-draw loop.

    Args:
        block_weights: Series indexed by `block_geoid` of development
            probabilities (draw weights); must be non-negative.
        block_capacity: Series indexed by `block_geoid` of deliverable
            residential-unit capacity; reindexed onto `block_weights` and treated
            as zero where missing.
        regional_target_units: Total residential units to place across all blocks.
        random_state: Seed for the draw, forwarded to `numpy.random.default_rng`;
            pass a fixed value for reproducible A/B comparisons.

    Returns:
        An integer Series indexed by `block_weights.index` giving the units
        allocated to each block (zero for blocks that received none).  The sum
        equals `regional_target_units` unless total eligible capacity is smaller,
        in which case it equals total eligible capacity.

    Example:
        >>> import pandas as pd
        >>> weights = pd.Series([0.9, 0.1], index=['A', 'B'])
        >>> capacity = pd.Series([10, 10], index=['A', 'B'])
        >>> allocation = allocate_units_to_blocks(weights, capacity, 5, random_state=0)
        >>> int(allocation.sum())
        5

    See Also:
        score_blocks: produces the `block_weights` development surface.
        block_residential_developer: renders these allocations down to parcels.
    """
    rng = np.random.default_rng(random_state)

    weights = block_weights.astype(float)
    capacity = block_capacity.reindex(weights.index).fillna(0.0)
    integer_capacity = np.floor(capacity).astype(int)

    eligible_mask = (integer_capacity >= 1) & (weights > 0)
    eligible_weights = weights[eligible_mask]
    eligible_capacity = integer_capacity[eligible_mask]

    allocated = pd.Series(0, index=weights.index, dtype=int)
    remaining_need = int(round(regional_target_units))
    if remaining_need <= 0 or len(eligible_weights) == 0:
        return allocated

    draw_keys = rng.random(len(eligible_weights)) ** (1.0 / eligible_weights.to_numpy())
    draw_order = eligible_weights.index[np.argsort(-draw_keys)]

    capacity_by_block = eligible_capacity.to_dict()
    for block in draw_order:
        if remaining_need <= 0:
            break
        units_to_place = min(capacity_by_block[block], remaining_need)
        allocated[block] = units_to_place
        remaining_need -= units_to_place

    return allocated


def allocate_units_to_blocks_by_jurisdiction(block_weights, block_capacity,
                                             block_dominant_juris,
                                             jurisdiction_targets,
                                             regional_none_target,
                                             random_state=None):
    """Allocates development to blocks within per-jurisdiction target buckets.

    Mirrors the parcel developer's jurisdiction-limits behavior at block grain:
    each growth-capped jurisdiction gets its own rollover
    target allocated only across the blocks it dominates, and every remaining
    (uncapped) block shares the leftover regional target -- the block-native
    analogue of `residential_developer`'s `targets` list.  Each bucket is an
    independent sequential stochastic draw (`allocate_units_to_blocks`), so a
    block only ever receives units from the single jurisdiction bucket it belongs
    to and is never over-filled beyond its deliverable capacity.  In an oversupplied
    year the regional target is zero and `regional_none_target` is non-positive,
    so only the capped jurisdictions build -- exactly matching the parcel path.

    Args:
        block_weights: Series indexed by `block_geoid` of LCM development weights.
        block_capacity: Series indexed by `block_geoid` of deliverable capacity.
        block_dominant_juris: Series indexed by `block_geoid` giving each block's
            dominant jurisdiction name.
        jurisdiction_targets: Mapping of capped-jurisdiction name to its positive
            unit target for this year.
        regional_none_target: Units to allocate across blocks whose dominant
            jurisdiction is not in `jurisdiction_targets`; non-positive values
            place nothing (the oversupplied case).
        random_state: Base seed; each bucket draws from an independent child stream
            (via `numpy.random.SeedSequence`) for reproducible A/B comparisons.

    Returns:
        An integer Series indexed by `block_weights.index` of units allocated to
        each block, summed across all jurisdiction buckets.

    Example:
        >>> import pandas as pd
        >>> weights = pd.Series([0.5, 0.5, 0.5], index=['A', 'B', 'C'])
        >>> capacity = pd.Series([10, 10, 10], index=['A', 'B', 'C'])
        >>> juris = pd.Series(['X', 'X', 'Y'], index=['A', 'B', 'C'])
        >>> allocation = allocate_units_to_blocks_by_jurisdiction(
        ...     weights, capacity, juris, {'X': 4}, 0, random_state=0)
        >>> int(allocation.sum())
        4

    See Also:
        allocate_units_to_blocks: the per-bucket allocation kernel this orchestrates.
        _dominant_juris_for_blocks: produces the `block_dominant_juris` mapping.
    """
    dominant_juris = block_dominant_juris.reindex(block_weights.index)
    capped_jurisdictions = set(jurisdiction_targets)

    bucket_count = len(jurisdiction_targets) + 1
    if random_state is None:
        bucket_seeds = [None] * bucket_count
    else:
        bucket_seeds = list(np.random.SeedSequence(random_state).spawn(bucket_count))

    allocated = pd.Series(0, index=block_weights.index, dtype=int)

    for bucket_seed, (juris, target) in zip(bucket_seeds, jurisdiction_targets.items()):
        bucket_blocks = block_weights.index[dominant_juris == juris]
        if target <= 0 or len(bucket_blocks) == 0:
            continue
        allocated.loc[bucket_blocks] += allocate_units_to_blocks(
            block_weights.loc[bucket_blocks],
            block_capacity.reindex(bucket_blocks),
            target,
            random_state=bucket_seed)

    if regional_none_target > 0:
        none_blocks = block_weights.index[~dominant_juris.isin(capped_jurisdictions)]
        if len(none_blocks) > 0:
            allocated.loc[none_blocks] += allocate_units_to_blocks(
                block_weights.loc[none_blocks],
                block_capacity.reindex(none_blocks),
                regional_none_target,
                random_state=bucket_seeds[-1])

    return allocated


# ---------------------------------------------------------------------------- #
#                       Rendering bridge (parcels + buildings)                  #
# ---------------------------------------------------------------------------- #

def _dominant_block_for_parcels(parcel_block):
    """Maps each parcel to the single block holding the largest share of its area.

    The areal `parcels_block` crosswalk maps a parcel to every block it overlaps
    (non-unique `parcel_id` index).  Rendering must place a parcel's development in
    exactly one block, so each parcel is assigned to its dominant (largest-share)
    block.

    Args:
        parcel_block: DataFrame indexed by `parcel_id` (non-unique) with
            `block_geoid` and `parcel_block_share` columns.

    Returns:
        A Series indexed by unique `parcel_id` whose values are the dominant
        `block_geoid` for each parcel.
    """
    parcel_block_reset = parcel_block.reset_index()
    dominant_rows = parcel_block_reset.loc[
        parcel_block_reset.groupby('parcel_id')['parcel_block_share'].idxmax()]
    return dominant_rows.set_index('parcel_id')['block_geoid']


def _dominant_juris_for_blocks(parcel_block, parcel_juris):
    """Maps each block to the jurisdiction holding the largest share of its area.

    Blocks do not nest cleanly within jurisdictions, so each block is assigned a
    single dominant jurisdiction: the one whose overlapping parcels contribute the
    greatest summed `parcel_block_share` within that block.  This is the block-grain
    analogue of the parcel path's per-jurisdiction masking.

    Args:
        parcel_block: DataFrame indexed by `parcel_id` (non-unique) with
            `block_geoid` and `parcel_block_share` columns.
        parcel_juris: Series indexed by `parcel_id` giving each parcel's
            jurisdiction name.

    Returns:
        A Series indexed by `block_geoid` whose values are the dominant
        jurisdiction name for each block.

    See Also:
        allocate_units_to_blocks_by_jurisdiction: consumes this mapping to bucket
            blocks by jurisdiction before allocating each jurisdiction's target.
    """
    parcel_block_reset = parcel_block.reset_index()
    parcel_block_reset['juris'] = parcel_block_reset['parcel_id'].map(parcel_juris)
    juris_share = parcel_block_reset.groupby(
        ['block_geoid', 'juris'])['parcel_block_share'].sum().reset_index()
    dominant_rows = juris_share.loc[
        juris_share.groupby('block_geoid')['parcel_block_share'].idxmax()]
    return dominant_rows.set_index('block_geoid')['juris']


def _render_block_allocations(block_allocated_units, feasibility, parcels, buildings,
                              dominant_block, year, form_to_btype_func,
                              add_extra_columns_func):
    """Renders per-block unit allocations into fabricated building rows.

    For each block with a positive allocation, picks profitable feasibility parcels
    within that block (by profit, via `subsidies.profit_to_prob_func`) until the
    block's allocation is met, using a block-scoped `Developer` so picking is
    capped to that block's parcels.  The per-block picks are concatenated and put
    through the same post-pick processing `run_developer` applies (year, form,
    building type, stories, extra columns), then merged into the `buildings` table
    with a single `add_buildings` call.

    Args:
        block_allocated_units: Integer Series indexed by `block_geoid` of units to
            place per block (from `allocate_units_to_blocks`).
        feasibility: The orca `feasibility` table wrapper (parcel-indexed, with a
            `(form, attribute)` column MultiIndex).
        parcels: The orca `parcels` table wrapper.
        buildings: The orca `buildings` table wrapper.
        dominant_block: Series mapping `parcel_id` to its dominant `block_geoid`.
        year: Simulation year, written to `year_built` on new buildings.
        form_to_btype_func: Callback mapping a building row to its building type.
        add_extra_columns_func: Callback adding BAUS's standard developer columns.

    Returns:
        None. Adds fabricated buildings to the orca `buildings` table in place
        (no-op if no block delivered any units).
    """
    developed = block_allocated_units[block_allocated_units > 0]
    if len(developed) == 0:
        return

    feasibility_df = feasibility.to_frame()
    parcel_size = parcels.parcel_size
    ave_unit_size = parcels.ave_sqft_per_unit
    current_units = parcels.total_residential_units

    parcels_in_developed = dominant_block[dominant_block.isin(developed.index)]
    parcels_by_block = parcels_in_developed.groupby(parcels_in_developed).groups

    picked_frames = []
    for block, units in developed.items():
        block_parcels = parcels_by_block.get(block)
        if block_parcels is None:
            continue
        block_parcels = block_parcels.intersection(feasibility_df.index)
        if len(block_parcels) == 0:
            continue

        block_developer = Developer(feasibility_df.loc[block_parcels].copy())
        new_block_buildings = block_developer.pick(
            "residential",
            int(units),
            parcel_size.loc[block_parcels].copy(),
            ave_unit_size.loc[block_parcels].copy(),
            current_units.loc[block_parcels].copy(),
            profit_to_prob_func=subsidies.profit_to_prob_func,
        )
        if new_block_buildings is not None and len(new_block_buildings) > 0:
            picked_frames.append(new_block_buildings)

    if len(picked_frames) == 0:
        return

    new_buildings = pd.concat(picked_frames, ignore_index=True)
    new_buildings["year_built"] = year
    new_buildings["form"] = "residential"
    new_buildings["building_type_id"] = new_buildings.apply(form_to_btype_func, axis=1)
    new_buildings["stories"] = new_buildings.stories.apply(np.ceil)
    new_buildings = add_extra_columns_func(new_buildings)
    new_buildings["subsidized"] = False

    add_buildings(buildings, new_buildings)


# ---------------------------------------------------------------------------- #
#                       Primary entry point (orca step)                         #
# ---------------------------------------------------------------------------- #

@orca.step()
def block_residential_developer(feasibility, households, buildings, parcels, year,
                                developer_settings, run_setup, parcels_block,
                                parcels_zoning_calculations, parcels_geography,
                                limits_settings, form_to_btype_func,
                                add_extra_columns_func):
    """Allocates residential development to blocks, then renders it to parcels.

    Block-native alternative to `residential_developer`: it computes the same
    regional unit target, derives per-block deliverable capacity from this year's
    feasibility, scores blocks with the fitted LCM, draws blocks stochastically by
    that score (capped at deliverable capacity), and renders the resulting per-block
    unit counts down to parcels as fabricated buildings.  It is wired into the annual
    model list in place of `residential_developer` only when
    `run_setup["developer_geography"]` is `"block"`.

    Args:
        feasibility: The orca `feasibility` table wrapper.
        households: The orca `households` table wrapper.
        buildings: The orca `buildings` table wrapper.
        parcels: The orca `parcels` table wrapper.
        year: The current simulation year.
        developer_settings: The developer settings dict (`residential_developer`
            sub-dict supplies the target vacancy).
        run_setup: The run configuration dict.
        parcels_block: The areal parcel-to-block crosswalk table wrapper.
        parcels_zoning_calculations: The orca table carrying `zoned_du` /
            `zoned_du_underbuild` (used by the capacity roll-up).
        parcels_geography: The orca table carrying `juris_name` per parcel, used
            to derive each block's dominant jurisdiction and the per-juris targets.
        limits_settings: The development-limits dict; its `Residential` sub-dict
            supplies each capped jurisdiction's annual unit limit.
        form_to_btype_func: Injectable callback mapping a building row to its type.
        add_extra_columns_func: Injectable callback adding standard developer columns.

    Returns:
        None. Adds fabricated buildings to the orca `buildings` table.

    See Also:
        allocate_units_to_blocks: the block allocation kernel this orchestrates.
        build_block_supply: the roll-up supplying per-block capacity.
    """
    orca.eval_step("alt_feasibility")

    feas_path = os.path.join(
        orca.get_injectable("outputs_dir"),
        f'feasibility_block_residential_developer_start_{year}.csv')
    feasibility.to_frame().to_csv(feas_path)

    kwargs = developer_settings['residential_developer']
    if run_setup["residential_vacancy_rate_mods"]:
        res_vacancy = orca.get_table("residential_vacancy_rate_mods").to_frame()
        target_vacancy = res_vacancy.loc[year].st_res_vac
    else:
        target_vacancy = kwargs["target_vacancy"]

    regional_target_units = int(Developer.compute_units_to_build(
        len(households),
        buildings["residential_units"].sum(),
        target_vacancy))
    print(f"Block residential developer: regional target of {regional_target_units:,} units")

    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    buildings_df = buildings.to_frame(
        ["parcel_id", "residential_units", "deed_restricted_units",
         "non_residential_sqft", "job_spaces", "source"])
    zoning_df = parcels_zoning_calculations.to_frame(["zoned_du", "zoned_du_underbuild"])
    feasibility_after_policy = orca.get_injectable("feasibility_after_policy")
    block_supply = build_block_supply(
        parcel_block, buildings_df, zoning_df, feasibility_after_policy)
    block_capacity = block_supply["profitable_residential_units"]

    block_covariates = pd.read_csv(
        _BLOCK_COVARIATES_PATH, dtype={"block_geoid": np.int64}).set_index("block_geoid")
    with open(_SPEC_YAML_PATH) as spec_file:
        spec = yaml.safe_load(spec_file)
    block_weights = score_blocks(block_covariates, spec)

    # Per-jurisdiction rollover targets, computed exactly as the parcel developer
    # (`residential_developer` in baus/models.py) so cap-driven development in
    # oversupplied years matches the parcel path.  Each capped jurisdiction gets a
    # positive annual target; the leftover regional target flows to uncapped blocks.
    juris_name = parcels_geography.juris_name.reindex(parcels.index).fillna("Other")
    jurisdiction_targets = {}
    regional_none_target = regional_target_units
    if "Residential" in limits_settings:
        for juris, limit in limits_settings["Residential"].items():
            current_total = parcels.total_residential_units[
                (juris_name == juris) & (parcels.newest_building >= 2010)].sum()
            target = int((year - 2010 + 1) * limit - current_total)
            if target <= 0:
                continue
            jurisdiction_targets[juris] = target
            regional_none_target -= target

    block_dominant_juris = _dominant_juris_for_blocks(parcel_block, juris_name)

    block_allocated_units = allocate_units_to_blocks_by_jurisdiction(
        block_weights, block_capacity, block_dominant_juris,
        jurisdiction_targets, regional_none_target, random_state=year)
    print(f"Block residential developer: allocated "
          f"{int(block_allocated_units.sum()):,} units across "
          f"{int((block_allocated_units > 0).sum()):,} blocks "
          f"({len(jurisdiction_targets)} capped jurisdictions, "
          f"none-bucket target {regional_none_target:,})")

    dominant_block = _dominant_block_for_parcels(parcel_block)
    _render_block_allocations(
        block_allocated_units, feasibility, parcels, buildings, dominant_block,
        year, form_to_btype_func, add_extra_columns_func)
