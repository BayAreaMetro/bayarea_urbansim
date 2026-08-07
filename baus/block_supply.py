"""Block-level supply and price/rent roll-up tables for the BAUS block port.

This module holds the additive, live (per-year) block roll-ups that the Phase-3
demand-side respec consumes as choice-model inputs. Nothing in the base BAUS
simulation reads these tables yet, so registering this module changes no model
behavior — the tables are only evaluated once a block ELCM/HLCM step is wired to
read them (Phase-3 chunks 3.3 and 3.7).

Two roll-ups live here:

- **Commercial total-stock** (``block_commercial_stock``): total-stock
  ``job_spaces`` / ``non_residential_sqft`` across *all* buildings (base-year plus
  developer-built), rolled up to census blocks — the block ELCM's supply
  alternatives. This differs from ``baus.summaries.core_summaries.build_block_supply``,
  which sums only *realized new* supply (``source != 'h5_inputs'``) at summary
  years for reporting.
- **Hedonic price/rent** (``block_residential_price``, ``block_nonres_rent``):
  the existing unit hedonic outputs (``unit_residential_price`` /
  ``unit_residential_rent``) and the non-residential rent, aggregated to blocks as
  weighted means — the block price/rent covariate the ELCM and HLCM respecs read.
- **ELCM alternatives** (``block_elcm_alternatives``): a block-indexed table
  exposing exactly the covariates ``configs/location_choice/elcm.yaml``'s
  ``model_expression`` reads, plus ``job_spaces`` (supply) and integer
  ``vacant_job_spaces`` (vacancy), so the existing ELCM spec can choose among
  blocks unchanged (Phase-3 chunk 3.3). The accessibility covariates are
  materialized directly onto the block table by area-weighted roll-up rather than
  broadcast at simulate time.
- **HLCM alternatives** (``block_own_alternatives``, ``block_rent_alternatives``):
  tenure-split, block-indexed residential choice tables exposing exactly the
  covariates ``configs/location_choice/hlcm_owner.yaml`` /
  ``hlcm_renter.yaml`` (and their lowincome / ``_no_unplaced`` variants) read, at
  grain **block x deed_restricted** so the ``deed_restricted`` alternative filters
  resolve. ``num_units`` (supply) and ``vacant_units`` (vacancy) roll up by
  **dominant block** (option B, mirroring the ELCM) while the price/rent covariate
  and accessibility covariates are intensive means; a majority-TAZ ``submarket_id``
  is carried for price/rent equilibration (Phase-3 chunk 3.7).

The intensive covariates (rent, accessibility) use the areal ``parcels_block``
crosswalk: each parcel quantity is apportioned to every block the parcel overlaps by
its ``parcel_block_share``, so block means blend all overlapping parcels. The block
ELCM's extensive supply and vacancy (``job_spaces`` / ``vacant_job_spaces``), by
contrast, are rolled up by **dominant block** (each building assigned wholly to its
parcel's largest-share block), so a block's vacancy equals exactly the building slots
the rendering bridge (``baus.block_elcm.render_block_jobs_to_buildings``) can fill --
no unplaced tail from an areal-vs-dominant seam (option B).

See Also:
    baus.summaries.core_summaries.build_block_supply: the Phase-1 realized-supply /
        capacity reporting roll-up that this module's helpers mirror in structure.
"""

import orca
import pandas as pd

from baus.block_developer import _dominant_block_for_parcels

__all__ = [
    # Areal apportionment building block
    '_apportion_to_blocks',
    # Pure roll-up helpers (unit-testable, no orca)
    'build_block_commercial_stock',
    'build_block_residential_price',
    'build_block_nonres_rent',
    'build_block_accessibility',
    'build_block_placed_jobs',
    'build_block_job_capacity',
    # Dominant-block residential helpers (HLCM supply/price by tenure x DR)
    '_tenure_units_by_dominant_block',
    'build_block_unit_capacity',
    'build_block_residential_price_by_tenure',
    'build_block_submarket',
    # ELCM alternatives assembler (composes the helpers above)
    'build_block_elcm_alternatives',
    # HLCM alternatives assembler (tenure-split residential choice inputs)
    'build_block_hlcm_alternatives',
    # Live per-year orca tables (block choice-model inputs)
    'block_commercial_stock',
    'block_residential_price',
    'block_nonres_rent',
    'block_elcm_alternatives',
    'block_own_alternatives',
    'block_rent_alternatives',
]

# Accessibility / neighborhood covariates that ``elcm.yaml``'s model_expression
# reads besides ``non_residential_rent``. All are building-level values (node /
# tmnode accessibility variables broadcast onto buildings, plus the
# ``juris_ave_income`` building column); they are rolled up to blocks as an
# area-weighted mean. Order mirrors the elcm.yaml expression for readability.
ELCM_ACCESSIBILITY_COVARIATES = [
    'office_1500',
    'industrial_1500',
    'retail_1500',
    'jobs_45',
    'residential_units_1500',
    'residential_units_45',
    'ave_income_1500',
    'juris_ave_income',
    'embarcadero',
    'stanford',
]

# Exact column set the block ELCM alternatives table exposes: every
# model_expression covariate plus the supply (``job_spaces``) and vacancy
# (``vacant_job_spaces``) fields ``utils.lcm_simulate`` needs.
ELCM_ALTERNATIVE_COLUMNS = (
    ['non_residential_rent']
    + ELCM_ACCESSIBILITY_COVARIATES
    + ['job_spaces', 'vacant_job_spaces']
)

# Accessibility / neighborhood covariates the HLCM specs
# (``configs/location_choice/hlcm_owner.yaml`` / ``hlcm_renter.yaml``) read besides
# the tenure price/rent term. All are continuous location covariates -- ``jobs_45``
# and ``ave_income_1500`` are node/tmnode accessibility values, and ``embarcadero`` /
# ``pacheights`` / ``stanford`` are nearest-landmark *distances* (not 0/1 dummies) --
# so every one is rolled up to blocks as an area-weighted mean, exactly as the ELCM
# treats ``embarcadero`` / ``stanford``. Order mirrors the hlcm model_expression.
HLCM_ACCESSIBILITY_COVARIATES = [
    'jobs_45',
    'ave_income_1500',
    'embarcadero',
    'pacheights',
    'stanford',
]

# Exact column set the owner block alternatives table exposes: the owner
# model_expression covariates (``unit_residential_price`` plus the accessibility
# covariates), the supply (``num_units``) and vacancy (``vacant_units``) fields
# ``utils.lcm_simulate`` needs, the ``submarket_id`` used for price equilibration,
# and the ``tenure`` / ``deed_restricted`` / ``block_geoid`` keys the alternative
# filters and the (chunk 3.9) rendering bridge read.
HLCM_OWN_ALTERNATIVE_COLUMNS = (
    HLCM_ACCESSIBILITY_COVARIATES
    + ['unit_residential_price', 'num_units', 'vacant_units',
       'submarket_id', 'tenure', 'deed_restricted', 'block_geoid']
)

# Renter counterpart of ``HLCM_OWN_ALTERNATIVE_COLUMNS`` (rent term instead of price).
HLCM_RENT_ALTERNATIVE_COLUMNS = (
    HLCM_ACCESSIBILITY_COVARIATES
    + ['unit_residential_rent', 'num_units', 'vacant_units',
       'submarket_id', 'tenure', 'deed_restricted', 'block_geoid']
)


def _apportion_to_blocks(parcel_block, parcel_values):
    """Apportions parcel-indexed quantities to census blocks by area share.

    Broadcasts each parcel's values across every block the parcel overlaps,
    weights each by the parcel's ``parcel_block_share`` in that block, and sums to
    block totals. Extensive quantities (counts, floor area) are conserved across
    parcels that straddle block boundaries. Mirrors the nested helper in
    ``baus.summaries.core_summaries.build_block_supply``.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns; a parcel appears
            once per overlapping block.
        parcel_values: DataFrame indexed by ``parcel_id`` whose numeric columns are
            the extensive quantities to apportion.

    Returns:
        A DataFrame indexed by ``block_geoid`` with the same columns as
        ``parcel_values``, holding the area-weighted block sums. Parcels absent
        from the crosswalk are dropped (inner join).

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        ...     index=pd.Index([1, 1], name="parcel_id"))
        >>> parcel_values = pd.DataFrame(
        ...     {"job_spaces": [10]}, index=pd.Index([1], name="parcel_id"))
        >>> _apportion_to_blocks(parcel_block, parcel_values).loc["A", "job_spaces"]
        6.0

    See Also:
        build_block_commercial_stock: extensive roll-up that uses this directly.
        build_block_residential_price: weighted-mean roll-up that apportions both a
            weighted numerator and a weight denominator with this helper.
    """
    merged = parcel_block.join(parcel_values, how="inner")
    value_cols = list(parcel_values.columns)
    weighted = merged[value_cols].multiply(merged["parcel_block_share"], axis=0)
    weighted["block_geoid"] = merged["block_geoid"]
    return weighted.groupby("block_geoid")[value_cols].sum()


def build_block_commercial_stock(parcel_block, buildings):
    """Rolls total-stock commercial supply up to census blocks.

    Sums ``job_spaces`` and ``non_residential_sqft`` across *all* buildings — both
    base-year stock (``source == 'h5_inputs'``) and developer-built — grouped to
    parcels, then apportions each parcel total to the blocks it overlaps by
    ``parcel_block_share``. This is the total-stock supply the block ELCM chooses
    among, distinct from the realized-new-supply reporting roll-up in
    ``core_summaries.build_block_supply``.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        buildings: DataFrame with a ``parcel_id`` column and ``job_spaces`` and
            ``non_residential_sqft`` columns (one row per building). A ``source``
            column, if present, is ignored — the total-stock roll-up counts every
            building.

    Returns:
        A DataFrame indexed by ``block_geoid`` with ``job_spaces`` and
        ``non_residential_sqft`` columns holding the area-weighted block totals.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> buildings = pd.DataFrame({"parcel_id": [1], "job_spaces": [5],
        ...     "non_residential_sqft": [2000], "source": ["h5_inputs"]})
        >>> build_block_commercial_stock(parcel_block, buildings).loc["A", "job_spaces"]
        5.0

    See Also:
        block_commercial_stock: the orca table that feeds this helper live model
            tables.
    """
    parcel_stock = buildings.groupby("parcel_id")[
        ["job_spaces", "non_residential_sqft"]].sum()
    return _apportion_to_blocks(parcel_block, parcel_stock)


def build_block_residential_price(parcel_block, residential_units):
    """Rolls unit residential price and rent up to census blocks as weighted means.

    Aggregates the per-unit hedonic outputs (``unit_residential_price`` /
    ``unit_residential_rent``) to a unit-count-weighted mean per block: each
    parcel's price/rent sum and unit count are apportioned to the blocks it
    overlaps by ``parcel_block_share``, then the apportioned price/rent sum is
    divided by the apportioned unit count. Because each ``residential_units`` row is
    a single unit, this is the mean unit price/rent of the units falling in a block.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        residential_units: DataFrame with a ``parcel_id`` column and
            ``unit_residential_price`` and ``unit_residential_rent`` columns (one
            row per unit).

    Returns:
        A DataFrame indexed by ``block_geoid`` with ``unit_residential_price`` and
        ``unit_residential_rent`` columns holding the unit-weighted block means.
        Blocks with no units are absent.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> units = pd.DataFrame({"parcel_id": [1, 1],
        ...     "unit_residential_price": [100.0, 300.0],
        ...     "unit_residential_rent": [1.0, 3.0]})
        >>> build_block_residential_price(parcel_block, units).loc["A", "unit_residential_price"]
        200.0

    See Also:
        block_residential_price: the orca table that feeds this helper live model
            tables.
        build_block_nonres_rent: the sqft-weighted commercial-rent counterpart.
    """
    grouped = residential_units.groupby("parcel_id")
    parcel_agg = pd.DataFrame({
        "price_sum": grouped["unit_residential_price"].sum(),
        "rent_sum": grouped["unit_residential_rent"].sum(),
        "unit_count": grouped.size(),
    })
    apportioned = _apportion_to_blocks(parcel_block, parcel_agg)
    block_price = pd.DataFrame(index=apportioned.index)
    block_price["unit_residential_price"] = (
        apportioned["price_sum"] / apportioned["unit_count"])
    block_price["unit_residential_rent"] = (
        apportioned["rent_sum"] / apportioned["unit_count"])
    return block_price


def build_block_nonres_rent(parcel_block, buildings):
    """Rolls non-residential rent up to census blocks as a sqft-weighted mean.

    Aggregates building ``non_residential_rent`` to a floor-area-weighted mean per
    block: each parcel's rent-times-sqft product and non-residential sqft are
    apportioned to the blocks it overlaps by ``parcel_block_share``, then the
    apportioned rent-times-sqft is divided by the apportioned sqft. Floor-area
    weighting matches the $/sqft definition of the rent. Blocks with no
    non-residential floor area have no defined rent and yield NaN.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        buildings: DataFrame with a ``parcel_id`` column and ``non_residential_rent``
            and ``non_residential_sqft`` columns (one row per building).

    Returns:
        A DataFrame indexed by ``block_geoid`` with a ``non_residential_rent``
        column holding the sqft-weighted block mean rent. Blocks with zero
        non-residential sqft yield NaN.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1, 2], name="parcel_id"))
        >>> buildings = pd.DataFrame({"parcel_id": [1, 2],
        ...     "non_residential_rent": [10.0, 20.0],
        ...     "non_residential_sqft": [100.0, 300.0]})
        >>> round(build_block_nonres_rent(parcel_block, buildings).loc["A", "non_residential_rent"], 2)
        17.5

    See Also:
        block_nonres_rent: the orca table that feeds this helper live model tables.
        build_block_residential_price: the unit-weighted residential counterpart.
    """
    weighted = buildings.copy()
    weighted["rent_times_sqft"] = (
        weighted["non_residential_rent"] * weighted["non_residential_sqft"])
    parcel_agg = weighted.groupby("parcel_id")[
        ["rent_times_sqft", "non_residential_sqft"]].sum()
    apportioned = _apportion_to_blocks(parcel_block, parcel_agg)
    block_rent = pd.DataFrame(index=apportioned.index)
    block_rent["non_residential_rent"] = (
        apportioned["rent_times_sqft"] / apportioned["non_residential_sqft"])
    return block_rent


def build_block_accessibility(parcel_block, buildings, covariate_cols):
    """Rolls building accessibility covariates up to census blocks as area means.

    Aggregates each intensive (per-location) covariate to an area-weighted block
    mean: building values are collapsed to a per-parcel mean, then each parcel mean
    is apportioned to the blocks the parcel overlaps by ``parcel_block_share`` and
    divided by the apportioned share so the result is an intensive mean (a rate,
    not a sum). Skeleton simplification: buildings are collapsed to their parcel by
    an unweighted mean (not floor-area weighted) and parcels enter the block mean
    weighted only by ``parcel_block_share`` (the only areal weight the crosswalk
    exposes), not by absolute intersection area.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        buildings: DataFrame with a ``parcel_id`` column and every column named in
            ``covariate_cols`` (one row per building).
        covariate_cols: List of building covariate column names to roll up.

    Returns:
        A DataFrame indexed by ``block_geoid`` with one column per entry in
        ``covariate_cols`` holding the area-weighted block mean.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A", "A"], "parcel_block_share": [1.0, 1.0]},
        ...     index=pd.Index([1, 2], name="parcel_id"))
        >>> buildings = pd.DataFrame({"parcel_id": [1, 2], "office_1500": [10.0, 20.0]})
        >>> build_block_accessibility(parcel_block, buildings, ["office_1500"]).loc["A", "office_1500"]
        15.0

    See Also:
        build_block_elcm_alternatives: composes this with the supply and rent
            roll-ups into the block ELCM alternatives table.
    """
    parcel_cov = buildings.groupby("parcel_id")[covariate_cols].mean()
    parcel_cov = parcel_cov.assign(_weight=1.0)
    apportioned = _apportion_to_blocks(parcel_block, parcel_cov)
    return apportioned[covariate_cols].divide(apportioned["_weight"], axis=0)


def build_block_placed_jobs(parcel_block, buildings, jobs):
    """Rolls placed jobs up to census blocks by area share.

    Counts jobs currently located in a building (``building_id != -1``), maps each
    to its building's parcel, and apportions those counts to the blocks the parcel
    overlaps by ``parcel_block_share``. The result is the (fractional) number of
    placed jobs occupying each block, used to derive block vacancy.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        buildings: DataFrame indexed by ``building_id`` with a ``parcel_id`` column.
        jobs: DataFrame with a ``building_id`` column (one row per job); the sentinel
            ``-1`` marks an unplaced job.

    Returns:
        A Series indexed by ``block_geoid`` giving the area-apportioned count of
        placed jobs per block. Blocks with no placed jobs are absent.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> buildings = pd.DataFrame({"parcel_id": [1]},
        ...     index=pd.Index([100], name="building_id"))
        >>> jobs = pd.DataFrame({"building_id": [100, 100, -1]})
        >>> build_block_placed_jobs(parcel_block, buildings, jobs).loc["A"]
        2.0

    See Also:
        build_block_elcm_alternatives: subtracts this from block ``job_spaces`` to
            derive ``vacant_job_spaces``.
    """
    placed = jobs.loc[jobs["building_id"] != -1, ["building_id"]].copy()
    placed["parcel_id"] = placed["building_id"].map(buildings["parcel_id"])
    parcel_counts = placed.groupby("parcel_id").size().to_frame("placed_job_spaces")
    apportioned = _apportion_to_blocks(parcel_block, parcel_counts)
    return apportioned["placed_job_spaces"]


def build_block_job_capacity(parcel_block, buildings):
    """Rolls building job-space supply and vacancy up to blocks by dominant block.

    Unlike the areal covariate roll-ups, block job-space supply (``job_spaces``) and
    vacancy (``vacant_job_spaces``) are summed over the buildings a block *actually
    holds*: each building is assigned wholly to its parcel's dominant (largest
    ``parcel_block_share``) block, exactly as ``baus.block_elcm`` renders block-placed
    jobs back to buildings. Summing the building-level ``vacant_job_spaces`` (already
    capacity-minus-placed, clipped per building) means the block vacancy the ELCM sees
    equals the exact pool of building slots the rendering bridge can fill, so every
    block-placed job is renderable and no unplaced tail arises from an areal-vs-
    dominant seam (option B). Buildings on parcels absent from the crosswalk have no
    dominant block and are dropped, mirroring the rendering bridge.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        buildings: DataFrame indexed by ``building_id`` with a ``parcel_id`` column
            and integer ``job_spaces`` (supply) and ``vacant_job_spaces`` (vacancy)
            columns.

    Returns:
        A DataFrame indexed by ``block_geoid`` (dtype matching the crosswalk's
        ``block_geoid``) with ``job_spaces`` and ``vacant_job_spaces`` block totals
        summed over the buildings assigned to each dominant block.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        ...     index=pd.Index([1, 1], name="parcel_id"))
        >>> buildings = pd.DataFrame(
        ...     {"parcel_id": [1], "job_spaces": [10], "vacant_job_spaces": [8]},
        ...     index=pd.Index([100], name="building_id"))
        >>> build_block_job_capacity(parcel_block, buildings).loc["A", "vacant_job_spaces"]
        8

    See Also:
        build_block_elcm_alternatives: composes this dominant-block supply/vacancy
            with the areal rent and accessibility covariates.
        baus.block_developer._dominant_block_for_parcels: the parcel-to-dominant-block
            mapping this roll-up and the rendering bridge share.
    """
    dominant_block = _dominant_block_for_parcels(parcel_block)
    building_block = buildings["parcel_id"].map(dominant_block)
    on_crosswalk = building_block.notna()
    capacity = buildings.loc[on_crosswalk, ["job_spaces", "vacant_job_spaces"]].copy()
    capacity["block_geoid"] = building_block[on_crosswalk].astype(dominant_block.dtype)
    return capacity.groupby("block_geoid")[["job_spaces", "vacant_job_spaces"]].sum()


def build_block_elcm_alternatives(parcel_block, buildings):
    """Assembles the block ELCM alternatives table from the supply/rent/access roll-ups.

    Composes the block-level covariates the base ELCM spec
    (``configs/location_choice/elcm.yaml``) reads into a single block-indexed table:
    ``non_residential_rent`` (sqft-weighted mean) and the accessibility covariates
    (area-weighted mean) from the **areal** crosswalk, plus ``job_spaces`` (supply)
    and integer ``vacant_job_spaces`` (vacancy) rolled up by **dominant block**.
    Rolling supply/vacancy by dominant block ties the per-block vacancy cap to the
    exact building slots the rendering bridge fills, so every block-placed job is
    renderable (no unplaced tail). ``job_spaces`` is not in the model_expression, so
    this changes only the vacancy caps, not block attractiveness.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        buildings: DataFrame indexed by ``building_id`` with a ``parcel_id`` column,
            ``job_spaces``, ``vacant_job_spaces``, ``non_residential_sqft``,
            ``non_residential_rent``, and every column named in
            ``ELCM_ACCESSIBILITY_COVARIATES``.

    Returns:
        A DataFrame indexed by ``block_geoid`` with exactly the columns in
        ``ELCM_ALTERNATIVE_COLUMNS``: every ``elcm.yaml`` model_expression covariate
        plus ``job_spaces`` and integer ``vacant_job_spaces``.

    See Also:
        block_elcm_alternatives: the orca table that feeds this helper live model
            tables.
        build_block_accessibility: the accessibility-covariate roll-up.
        build_block_job_capacity: the dominant-block supply/vacancy roll-up.
    """
    nonres_rent = build_block_nonres_rent(parcel_block, buildings)
    accessibility = build_block_accessibility(
        parcel_block, buildings, ELCM_ACCESSIBILITY_COVARIATES)
    capacity = build_block_job_capacity(parcel_block, buildings)

    alternatives = capacity.join([nonres_rent, accessibility])
    # Blocks with zero non-residential sqft yield NaN rent (0/0 in the sqft-weighted
    # mean). Fill with 0 so lcm_simulate's check_nas passes; np.log1p(0) == 0 mirrors
    # the building-level convention (residential locations carry rent 0, not NaN).
    alternatives["non_residential_rent"] = alternatives["non_residential_rent"].fillna(0)
    alternatives["vacant_job_spaces"] = alternatives["vacant_job_spaces"].astype(int)
    return alternatives[ELCM_ALTERNATIVE_COLUMNS]


def _tenure_units_by_dominant_block(parcel_block, residential_units, tenure):
    """Filters residential units to one tenure and tags each with its dominant block.

    Assigns every residential unit wholly to its parcel's dominant (largest
    ``parcel_block_share``) block -- the same parcel-to-block mapping the ELCM supply
    roll-up and the block rendering bridge use -- and keeps only the units of the
    requested tenure. Units on parcels absent from the crosswalk have no dominant
    block and are dropped, mirroring the rendering bridge. This is the shared front
    end for the dominant-block residential roll-ups (capacity and price).

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        residential_units: DataFrame with a ``parcel_id`` column and a ``tenure``
            column (one row per unit), plus whatever value columns the caller rolls
            up.
        tenure: The tenure to keep, ``"own"`` or ``"rent"``.

    Returns:
        A copy of the requested-tenure, on-crosswalk rows of ``residential_units``
        with an added ``block_geoid`` column (dtype matching the crosswalk's
        ``block_geoid``) giving each unit's dominant block.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> units = pd.DataFrame({"parcel_id": [1, 1], "tenure": ["own", "rent"]})
        >>> _tenure_units_by_dominant_block(parcel_block, units, "own")["block_geoid"].tolist()
        ['A']

    See Also:
        build_block_unit_capacity: dominant-block supply/vacancy roll-up built on this.
        build_block_residential_price_by_tenure: dominant-block price roll-up built on
            this.
        baus.block_developer._dominant_block_for_parcels: the parcel-to-dominant-block
            mapping this roll-up shares with the rendering bridge.
    """
    dominant_block = _dominant_block_for_parcels(parcel_block)
    units = residential_units[residential_units["tenure"] == tenure].copy()
    block = units["parcel_id"].map(dominant_block)
    on_crosswalk = block.notna()
    units = units.loc[on_crosswalk].copy()
    units["block_geoid"] = block[on_crosswalk].astype(dominant_block.dtype)
    return units


def build_block_unit_capacity(parcel_block, residential_units, tenure):
    """Rolls residential unit supply and vacancy up to blocks by dominant block.

    Sums ``num_units`` (supply) and ``vacant_units`` (vacancy) over the units a block
    *actually holds*: each unit is assigned wholly to its parcel's dominant block and
    grouped by ``(block_geoid, deed_restricted)``. Rolling supply/vacancy by dominant
    block ties the per-block vacancy cap to the exact residential-unit slots the
    (chunk 3.9) rendering bridge can fill, so every block-placed household is
    renderable and no unplaced tail arises from an areal-vs-dominant seam (option B,
    mirroring ``build_block_job_capacity`` on the ELCM side). The ``deed_restricted``
    split is required because the lowincome / ``_no_unplaced`` HLCM specs filter
    alternatives on ``deed_restricted``.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        residential_units: DataFrame with a ``parcel_id`` column and ``tenure``,
            ``deed_restricted``, ``num_units``, and ``vacant_units`` columns (one row
            per unit).
        tenure: The tenure to roll up, ``"own"`` or ``"rent"``.

    Returns:
        A DataFrame indexed by a ``(block_geoid, deed_restricted)`` MultiIndex with
        integer ``num_units`` and ``vacant_units`` block totals over the units
        assigned to each dominant block.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> units = pd.DataFrame({"parcel_id": [1, 1], "tenure": ["own", "own"],
        ...     "deed_restricted": [0.0, 0.0], "num_units": [1, 1],
        ...     "vacant_units": [1, 0]})
        >>> build_block_unit_capacity(parcel_block, units, "own").loc[("A", 0.0), "vacant_units"]
        1

    See Also:
        build_block_hlcm_alternatives: composes this dominant-block supply/vacancy with
            the tenure price and accessibility covariates.
        build_block_job_capacity: the ELCM commercial counterpart.
    """
    units = _tenure_units_by_dominant_block(parcel_block, residential_units, tenure)
    grouped = units.groupby(["block_geoid", "deed_restricted"])
    return pd.DataFrame({
        "num_units": grouped["num_units"].sum().astype(int),
        "vacant_units": grouped["vacant_units"].sum().astype(int),
    })


def build_block_residential_price_by_tenure(parcel_block, residential_units, tenure,
                                            price_col):
    """Rolls a tenure's unit price or rent up to blocks as a unit-weighted mean.

    Aggregates the per-unit hedonic value in ``price_col`` to a unit mean per
    ``(block_geoid, deed_restricted)`` group, over only the units of the requested
    tenure assigned to each dominant block. Owner blocks average
    ``unit_residential_price``; renter blocks average ``unit_residential_rent`` -- the
    tenure-specific covariate the owner and renter HLCM specs read, matching how the
    parcel path chooses over tenure-filtered ``own_units`` / ``rent_units``.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        residential_units: DataFrame with a ``parcel_id`` column and ``tenure``,
            ``deed_restricted``, and ``price_col`` columns (one row per unit).
        tenure: The tenure to roll up, ``"own"`` or ``"rent"``.
        price_col: The per-unit value column to average, ``"unit_residential_price"``
            (owner) or ``"unit_residential_rent"`` (renter).

    Returns:
        A DataFrame indexed by a ``(block_geoid, deed_restricted)`` MultiIndex with a
        single ``price_col`` column holding the unit-weighted block mean.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> units = pd.DataFrame({"parcel_id": [1, 1], "tenure": ["own", "own"],
        ...     "deed_restricted": [0.0, 0.0],
        ...     "unit_residential_price": [100.0, 300.0]})
        >>> build_block_residential_price_by_tenure(
        ...     parcel_block, units, "own", "unit_residential_price"
        ... ).loc[("A", 0.0), "unit_residential_price"]
        200.0

    See Also:
        build_block_hlcm_alternatives: composes this with the dominant-block capacity
            and accessibility covariates.
        build_block_residential_price: the tenure-blind unit-weighted counterpart.
    """
    units = _tenure_units_by_dominant_block(parcel_block, residential_units, tenure)
    grouped = units.groupby(["block_geoid", "deed_restricted"])
    return pd.DataFrame({price_col: grouped[price_col].mean()})


def build_block_submarket(parcel_block, residential_units):
    """Assigns each census block its majority-TAZ submarket id.

    Maps every residential unit to its parcel's dominant block and picks, per block,
    the ``zone_id`` (TAZ) holding the most units -- the block's majority submarket.
    Price/rent equilibration operates at the TAZ ``submarket_id`` grain, so collapsing
    a block that spans TAZs to its plurality TAZ keeps equilibration at the existing
    grain rather than introducing a new block-level submarket. Ties are broken by the
    smallest ``zone_id`` for determinism. All units are used (tenure-blind) so the
    owner and renter alternatives tables agree on a block's submarket.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        residential_units: DataFrame with a ``parcel_id`` column and a ``zone_id``
            column (one row per unit).

    Returns:
        A Series indexed by ``block_geoid`` (dtype matching the crosswalk's
        ``block_geoid``) giving each block's integer majority-TAZ ``submarket_id``.
        Blocks with no on-crosswalk units are absent.

    Example:
        >>> import pandas as pd
        >>> parcel_block = pd.DataFrame(
        ...     {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        ...     index=pd.Index([1], name="parcel_id"))
        >>> units = pd.DataFrame({"parcel_id": [1, 1, 1], "zone_id": [7, 7, 9]})
        >>> build_block_submarket(parcel_block, units).loc["A"]
        7

    See Also:
        build_block_hlcm_alternatives: carries this ``submarket_id`` onto the
            alternatives table for price/rent equilibration.
    """
    dominant_block = _dominant_block_for_parcels(parcel_block)
    block = residential_units["parcel_id"].map(dominant_block)
    on_crosswalk = block.notna()
    units = residential_units.loc[on_crosswalk, ["zone_id"]].copy()
    units["block_geoid"] = block[on_crosswalk].astype(dominant_block.dtype)

    def _majority_zone(zone_ids):
        counts = zone_ids.value_counts()
        tied = counts.index[counts == counts.max()]
        return int(min(tied))

    return units.groupby("block_geoid")["zone_id"].agg(_majority_zone)


def build_block_hlcm_alternatives(parcel_block, residential_units, buildings, tenure):
    """Assembles a tenure's block HLCM alternatives table from the residential roll-ups.

    Composes the block-level covariates the owner or renter HLCM spec
    (``configs/location_choice/hlcm_owner.yaml`` / ``hlcm_renter.yaml``) reads into a
    single alternatives table at grain ``block x deed_restricted``: dominant-block
    ``num_units`` (supply) and ``vacant_units`` (vacancy); the tenure price/rent
    (unit-weighted mean); the accessibility covariates in
    ``HLCM_ACCESSIBILITY_COVARIATES`` (area-weighted mean, broadcast across the
    deed-restricted split); a majority-TAZ ``submarket_id``; and the ``tenure`` /
    ``deed_restricted`` / ``block_geoid`` keys the alternative filters and the rendering
    bridge read. Rolling supply/vacancy by dominant block ties the per-block vacancy cap
    to the exact unit slots the rendering bridge fills (option B); the price/rent and
    accessibility covariates are intensive means. The frame is indexed by a plain
    ``block_alt_id`` (one row per ``(block, deed_restricted)`` group) so
    ``lcm_simulate``'s ``deed_restricted`` alternative filters resolve against a flat
    column and the rendering bridge can key on ``(block_geoid, deed_restricted)``.

    Pure (no orca, no file I/O) so it can be unit-tested directly.

    Args:
        parcel_block: DataFrame indexed by ``parcel_id`` (non-unique) with
            ``block_geoid`` and ``parcel_block_share`` columns.
        residential_units: DataFrame with ``parcel_id``, ``zone_id``, ``tenure``,
            ``deed_restricted``, ``num_units``, ``vacant_units``, and the tenure price
            column (``unit_residential_price`` for owner, ``unit_residential_rent`` for
            renter) columns (one row per unit).
        buildings: DataFrame with a ``parcel_id`` column and every column named in
            ``HLCM_ACCESSIBILITY_COVARIATES`` (one row per building).
        tenure: The tenure to build, ``"own"`` or ``"rent"``.

    Returns:
        A DataFrame indexed by ``block_alt_id`` with exactly the columns in
        ``HLCM_OWN_ALTERNATIVE_COLUMNS`` (owner) or ``HLCM_RENT_ALTERNATIVE_COLUMNS``
        (renter): the spec covariates, integer ``num_units`` / ``vacant_units``,
        ``submarket_id``, and the ``tenure`` / ``deed_restricted`` / ``block_geoid``
        keys. One row per ``(block, deed_restricted)`` group of the given tenure.

    See Also:
        block_own_alternatives: the orca table that feeds this helper live model tables
            for owner households.
        block_rent_alternatives: the renter orca table.
        build_block_unit_capacity: the dominant-block supply/vacancy roll-up.
        build_block_residential_price_by_tenure: the tenure price roll-up.
        build_block_elcm_alternatives: the ELCM commercial counterpart.
    """
    price_col = ("unit_residential_price" if tenure == "own"
                 else "unit_residential_rent")
    columns = (HLCM_OWN_ALTERNATIVE_COLUMNS if tenure == "own"
               else HLCM_RENT_ALTERNATIVE_COLUMNS)

    capacity = build_block_unit_capacity(parcel_block, residential_units, tenure)
    price = build_block_residential_price_by_tenure(
        parcel_block, residential_units, tenure, price_col)
    accessibility = build_block_accessibility(
        parcel_block, buildings, HLCM_ACCESSIBILITY_COVARIATES)
    submarket = build_block_submarket(parcel_block, residential_units)

    alternatives = capacity.join(price).reset_index()
    alternatives = alternatives.merge(
        accessibility, left_on="block_geoid", right_index=True, how="left")
    alternatives["submarket_id"] = alternatives["block_geoid"].map(submarket)
    alternatives["tenure"] = tenure
    alternatives["deed_restricted"] = alternatives["deed_restricted"].astype(bool)
    alternatives.index.name = "block_alt_id"
    return alternatives[columns]


@orca.table(cache=False)
def block_commercial_stock(buildings, parcels_block):
    """Live per-year census-block roll-up of total-stock commercial supply.

    Additive block-choice input: reads the ``buildings`` table and the areal
    ``parcels_block`` crosswalk and returns total-stock ``job_spaces`` /
    ``non_residential_sqft`` per 2020 census block. Recomputed each year
    (``cache=False``). Nothing consumes it until the block ELCM is wired (Phase-3
    chunk 3.3), so registering it changes no model behavior.

    Args:
        buildings: The orca ``buildings`` table.
        parcels_block: The areal parcel-to-block crosswalk table.

    Returns:
        A DataFrame indexed by ``block_geoid`` with ``job_spaces`` and
        ``non_residential_sqft`` block totals.

    See Also:
        build_block_commercial_stock: the pure helper that performs the roll-up.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    buildings_df = buildings.to_frame(
        ["parcel_id", "job_spaces", "non_residential_sqft"])
    return build_block_commercial_stock(parcel_block, buildings_df)


@orca.table(cache=False)
def block_residential_price(residential_units, buildings, parcels_block):
    """Live per-year census-block roll-up of unit residential price and rent.

    Additive block-choice input: joins each residential unit to its building's
    parcel, then rolls the per-unit hedonic price/rent up to 2020 census blocks as
    a unit-weighted mean via the areal ``parcels_block`` crosswalk. Recomputed each
    year (``cache=False``). Nothing consumes it until the block HLCM is wired
    (Phase-3 chunk 3.7), so registering it changes no model behavior.

    Args:
        residential_units: The orca ``residential_units`` table (unit grain, with
            ``building_id``, ``unit_residential_price``, ``unit_residential_rent``).
        buildings: The orca ``buildings`` table, used to map ``building_id`` to
            ``parcel_id``.
        parcels_block: The areal parcel-to-block crosswalk table.

    Returns:
        A DataFrame indexed by ``block_geoid`` with ``unit_residential_price`` and
        ``unit_residential_rent`` unit-weighted block means.

    See Also:
        build_block_residential_price: the pure helper that performs the roll-up.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    units = residential_units.to_frame(
        ["building_id", "unit_residential_price", "unit_residential_rent"])
    units["parcel_id"] = units["building_id"].map(buildings.to_frame(["parcel_id"])["parcel_id"])
    return build_block_residential_price(parcel_block, units)


@orca.table(cache=False)
def block_nonres_rent(buildings, parcels_block):
    """Live per-year census-block roll-up of non-residential rent.

    Additive block-choice input: rolls building ``non_residential_rent`` up to 2020
    census blocks as a floor-area-weighted mean via the areal ``parcels_block``
    crosswalk. Recomputed each year (``cache=False``). Nothing consumes it until the
    block ELCM is wired (Phase-3 chunk 3.3), so registering it changes no model
    behavior.

    Args:
        buildings: The orca ``buildings`` table.
        parcels_block: The areal parcel-to-block crosswalk table.

    Returns:
        A DataFrame indexed by ``block_geoid`` with a ``non_residential_rent``
        sqft-weighted block mean.

    See Also:
        build_block_nonres_rent: the pure helper that performs the roll-up.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    buildings_df = buildings.to_frame(
        ["parcel_id", "non_residential_rent", "non_residential_sqft"])
    return build_block_nonres_rent(parcel_block, buildings_df)


@orca.table(cache=False)
def block_elcm_alternatives(parcels_block):
    """Live per-year block alternatives table for the block ELCM respec.

    Additive block-choice input: assembles the covariates the base ELCM spec
    (``configs/location_choice/elcm.yaml``) reads into a block-indexed alternatives
    table — ``non_residential_rent`` (sqft-weighted), the accessibility covariates
    in ``ELCM_ACCESSIBILITY_COVARIATES`` (area-weighted), ``job_spaces`` (supply),
    and integer ``vacant_job_spaces`` (vacancy). Supply and vacancy are summed over
    each building's dominant block (via the building-level ``vacant_job_spaces``
    column) so the per-block vacancy cap matches the rendering bridge's building
    slots exactly. Recomputed each year (``cache=False``).

    The accessibility covariates (``office_1500``, ``retail_1500``, …) live on the
    ``nodes`` / ``tmnodes`` accessibility tables and reach buildings only through
    orca broadcasts, so the building-level covariate frame is assembled with
    ``orca.merge_tables`` — the same mechanism ``utils.lcm_simulate`` uses via its
    ``join_tbls`` argument — rather than ``buildings.to_frame``, which would not
    resolve the broadcast columns. They are then materialized directly onto the
    block table (area-weighted roll-up), so the block ELCM step needs no
    ``join_tbls`` broadcast of its own.

    Args:
        parcels_block: The areal parcel-to-block crosswalk table.

    Returns:
        A DataFrame indexed by ``block_geoid`` with exactly the columns in
        ``ELCM_ALTERNATIVE_COLUMNS``.

    See Also:
        build_block_elcm_alternatives: the pure helper that performs the assembly.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    building_cols = (
        ["parcel_id", "job_spaces", "vacant_job_spaces",
         "non_residential_sqft", "non_residential_rent"]
        + ELCM_ACCESSIBILITY_COVARIATES)
    buildings_df = orca.merge_tables(
        "buildings", ["buildings", "nodes", "tmnodes"], columns=building_cols)
    return build_block_elcm_alternatives(parcel_block, buildings_df)


def _block_hlcm_alternatives(residential_units, buildings, parcels_block, tenure):
    """Reads the orca tables and builds one tenure's block HLCM alternatives table.

    Shared body for the ``block_own_alternatives`` and ``block_rent_alternatives``
    orca tables. Materializes the residential-unit and building-covariate frames the
    pure assembler needs: units carry their per-unit price/rent, tenure,
    deed-restricted flag, supply/vacancy, TAZ, and (mapped) parcel id; buildings carry
    the accessibility covariates. The accessibility covariates (``jobs_45``,
    ``embarcadero``, …) live on the ``nodes`` / ``tmnodes`` tables and reach buildings
    only through orca broadcasts, so the building frame is assembled with
    ``orca.merge_tables`` -- the same mechanism ``utils.lcm_simulate`` uses via
    ``join_tbls`` -- rather than ``buildings.to_frame``, which would not resolve the
    broadcast columns. The covariates are then materialized onto the block table, so
    the (chunk 3.8) block HLCM step needs no ``join_tbls`` broadcast of its own.

    Args:
        residential_units: The orca ``residential_units`` table (unit grain).
        buildings: The orca ``buildings`` table, used to map ``building_id`` to
            ``parcel_id``.
        parcels_block: The areal parcel-to-block crosswalk table.
        tenure: The tenure to build, ``"own"`` or ``"rent"``.

    Returns:
        A DataFrame indexed by ``block_alt_id`` with exactly the columns in
        ``HLCM_OWN_ALTERNATIVE_COLUMNS`` (owner) or ``HLCM_RENT_ALTERNATIVE_COLUMNS``
        (renter).

    See Also:
        build_block_hlcm_alternatives: the pure helper that performs the assembly.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    price_col = ("unit_residential_price" if tenure == "own"
                 else "unit_residential_rent")
    units = residential_units.to_frame(
        ["building_id", "zone_id", "tenure", "deed_restricted",
         "num_units", "vacant_units", price_col])
    building_cols = ["parcel_id"] + HLCM_ACCESSIBILITY_COVARIATES
    buildings_df = orca.merge_tables(
        "buildings", ["buildings", "nodes", "tmnodes"], columns=building_cols)
    units["parcel_id"] = units["building_id"].map(buildings_df["parcel_id"])
    return build_block_hlcm_alternatives(parcel_block, units, buildings_df, tenure)


@orca.table(cache=False)
def block_own_alternatives(residential_units, buildings, parcels_block):
    """Live per-year owner block alternatives table for the block HLCM respec.

    Additive block-choice input: assembles the covariates the owner HLCM specs
    (``configs/location_choice/hlcm_owner.yaml`` and its lowincome / ``_no_unplaced``
    variants) read into a block-indexed alternatives table at grain
    ``block x deed_restricted`` -- ``unit_residential_price`` and the accessibility
    covariates (intensive means), dominant-block ``num_units`` / ``vacant_units``, a
    majority-TAZ ``submarket_id``, and the ``tenure`` / ``deed_restricted`` /
    ``block_geoid`` keys -- over owner residential units. Recomputed each year
    (``cache=False``). Nothing consumes it until the block HLCM step is wired (Phase-3
    chunk 3.8), so registering it changes no model behavior.

    Args:
        residential_units: The orca ``residential_units`` table.
        buildings: The orca ``buildings`` table (parcel id + accessibility covariates).
        parcels_block: The areal parcel-to-block crosswalk table.

    Returns:
        A DataFrame indexed by ``block_alt_id`` with the columns in
        ``HLCM_OWN_ALTERNATIVE_COLUMNS``.

    See Also:
        build_block_hlcm_alternatives: the pure helper that performs the assembly.
        block_rent_alternatives: the renter counterpart.
    """
    return _block_hlcm_alternatives(
        residential_units, buildings, parcels_block, "own")


@orca.table(cache=False)
def block_rent_alternatives(residential_units, buildings, parcels_block):
    """Live per-year renter block alternatives table for the block HLCM respec.

    Additive block-choice input: the renter counterpart of ``block_own_alternatives``,
    assembling the covariates the renter HLCM specs
    (``configs/location_choice/hlcm_renter.yaml`` and its lowincome / ``_no_unplaced``
    variants) read -- ``unit_residential_rent`` and the accessibility covariates,
    dominant-block ``num_units`` / ``vacant_units``, a majority-TAZ ``submarket_id``,
    and the ``tenure`` / ``deed_restricted`` / ``block_geoid`` keys -- over renter
    residential units. Recomputed each year (``cache=False``). Nothing consumes it
    until the block HLCM step is wired (Phase-3 chunk 3.8), so registering it changes
    no model behavior.

    Args:
        residential_units: The orca ``residential_units`` table.
        buildings: The orca ``buildings`` table (parcel id + accessibility covariates).
        parcels_block: The areal parcel-to-block crosswalk table.

    Returns:
        A DataFrame indexed by ``block_alt_id`` with the columns in
        ``HLCM_RENT_ALTERNATIVE_COLUMNS``.

    See Also:
        build_block_hlcm_alternatives: the pure helper that performs the assembly.
        block_own_alternatives: the owner counterpart.
    """
    return _block_hlcm_alternatives(
        residential_units, buildings, parcels_block, "rent")
