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

All roll-ups use the areal ``parcels_block`` crosswalk: each parcel quantity is
apportioned to every block the parcel overlaps by its ``parcel_block_share``, so
block totals conserve parcel totals across parcels that straddle block boundaries.

See Also:
    baus.summaries.core_summaries.build_block_supply: the Phase-1 realized-supply /
        capacity reporting roll-up that this module's helpers mirror in structure.
"""

import orca
import pandas as pd

__all__ = [
    # Areal apportionment building block
    '_apportion_to_blocks',
    # Pure roll-up helpers (unit-testable, no orca)
    'build_block_commercial_stock',
    'build_block_residential_price',
    'build_block_nonres_rent',
    # Live per-year orca tables (block choice-model inputs)
    'block_commercial_stock',
    'block_residential_price',
    'block_nonres_rent',
]


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
