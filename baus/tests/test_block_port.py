"""Standalone unit tests for the parcel->block port (Phase 1).

All tests target the pure roll-up helper ``build_block_supply`` so they run
without orca or file I/O. Consolidated here on purpose; may be reorganized later.

Run:
    pytest baus/tests/test_block_port.py
    python  baus/tests/test_block_port.py
"""

import numpy as np
import pandas as pd

from baus.summaries.core_summaries import build_block_supply
from baus.block_supply import (
    build_block_commercial_stock,
    build_block_residential_price,
    build_block_nonres_rent,
)


# --- Shared fixtures ---------------------------------------------------------

def _parcel_block():
    # areal crosswalk shape: parcel_id (may repeat) -> block_geoid with the
    # parcel's area share in that block. Here each parcel sits wholly in one
    # block (share 1.0), so block totals equal the parcel totals.
    return pd.DataFrame(
        {"block_geoid": ["A", "A", "B", "B"],
         "parcel_block_share": [1.0, 1.0, 1.0, 1.0]},
        index=pd.Index([1, 2, 3, 4], name="parcel_id"))


def _buildings():
    return pd.DataFrame({
        "parcel_id":             [1,           1,                 2,                 3,                 4],
        "residential_units":     [10,          5,                 8,                 4,                 20],
        "deed_restricted_units": [0,           1,                 2,                 0,                 0],
        "non_residential_sqft":  [0,           0,                 100,               50,                0],
        "job_spaces":            [0,           0,                 3,                 2,                 0],
        "source":                ["h5_inputs", "developer_model", "developer_model", "developer_model", "h5_inputs"],
    })


def _zoning():
    return pd.DataFrame(
        {"zoned_du": [100, 200, 50, 0], "zoned_du_underbuild": [40, 80, 10, 0]},
        index=pd.Index([1, 2, 3, 4], name="parcel_id"))


def _feasibility():
    cols = pd.MultiIndex.from_tuples([
        ("residential", "total_residential_units"),
        ("residential", "max_profit")])
    return pd.DataFrame(
        [[30, 5.0],      # p1 profitable   -> 30
         [40, -1.0],     # p2 unprofitable -> 0
         [20, np.nan],   # p3 infeasible   -> 0
         [10, 2.0]],     # p4 profitable   -> 10
        index=pd.Index([1, 2, 3, 4], name="parcel_id"), columns=cols)


# --- Tests -------------------------------------------------------------------

def test_realized_supply_excludes_base_stock():
    out = build_block_supply(_parcel_block(), _buildings(), _zoning(), _feasibility())
    # base-year h5_inputs rows (p1=10, p4=20) are dropped; only developer rows count
    assert out.loc["A", "built_residential_units"] == 13   # 5 + 8
    assert out.loc["B", "built_residential_units"] == 4


def test_profitable_capacity_gating():
    out = build_block_supply(_parcel_block(), _buildings(), _zoning(), _feasibility())
    # p1 kept (30), p2 negative profit -> 0, p3 NaN -> 0, p4 kept (10)
    assert out.loc["A", "profitable_residential_units"] == 30
    assert out.loc["B", "profitable_residential_units"] == 10


def test_zoned_capacity_rollup():
    out = build_block_supply(_parcel_block(), _buildings(), _zoning(), _feasibility())
    assert out.loc["A", "zoned_du"] == 300             # 100 + 200
    assert out.loc["A", "zoned_du_underbuild"] == 120  # 40 + 80
    assert out.loc["B", "zoned_du"] == 50


def test_unit_conservation():
    out = build_block_supply(_parcel_block(), _buildings(), _zoning(), _feasibility())
    # block totals equal the sum of the underlying non-base parcels
    assert out["built_residential_units"].sum() == 5 + 8 + 4
    assert out["built_non_residential_sqft"].sum() == 100 + 50
    assert out["built_job_spaces"].sum() == 3 + 2


def test_missing_block_filled_with_zero():
    # a block that only appears in zoning must show 0 (not NaN) for built/profitable
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "C"], "parcel_block_share": [1.0, 1.0]},
        index=pd.Index([1, 2], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id": [1], "residential_units": [7], "deed_restricted_units": [0],
        "non_residential_sqft": [0], "job_spaces": [0], "source": ["developer_model"]})
    zoning = pd.DataFrame(
        {"zoned_du": [100, 50], "zoned_du_underbuild": [0, 0]},
        index=pd.Index([1, 2], name="parcel_id"))
    cols = pd.MultiIndex.from_tuples([
        ("residential", "total_residential_units"), ("residential", "max_profit")])
    feasibility = pd.DataFrame(
        [[9, 1.0]], index=pd.Index([1], name="parcel_id"), columns=cols)

    out = build_block_supply(parcel_block, buildings, zoning, feasibility)
    assert out.loc["C", "built_residential_units"] == 0
    assert out.loc["C", "profitable_residential_units"] == 0
    assert out.loc["A", "built_residential_units"] == 7


def test_area_share_apportions_split_parcel():
    # a parcel straddling two blocks splits its supply and capacity by area share
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        index=pd.Index([1, 1], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id": [1], "residential_units": [10], "deed_restricted_units": [0],
        "non_residential_sqft": [0], "job_spaces": [0], "source": ["developer_model"]})
    zoning = pd.DataFrame(
        {"zoned_du": [100], "zoned_du_underbuild": [40]},
        index=pd.Index([1], name="parcel_id"))
    cols = pd.MultiIndex.from_tuples([
        ("residential", "total_residential_units"), ("residential", "max_profit")])
    feasibility = pd.DataFrame(
        [[30, 1.0]], index=pd.Index([1], name="parcel_id"), columns=cols)

    out = build_block_supply(parcel_block, buildings, zoning, feasibility)
    assert out.loc["A", "built_residential_units"] == 6.0   # 10 * 0.6
    assert out.loc["B", "built_residential_units"] == 4.0   # 10 * 0.4
    assert out.loc["A", "zoned_du"] == 60.0                 # 100 * 0.6
    assert out.loc["B", "zoned_du"] == 40.0                 # 100 * 0.4
    assert out.loc["A", "profitable_residential_units"] == 18.0  # 30 * 0.6
    assert out.loc["B", "profitable_residential_units"] == 12.0  # 30 * 0.4
    # apportionment conserves the parcel totals across the split
    assert out["built_residential_units"].sum() == 10.0
    assert out["profitable_residential_units"].sum() == 30.0


def test_crosswalk_shares_sum_to_one_per_parcel():
    # areal crosswalk invariant: a parcel's block shares sum to ~1
    xwalk = _parcel_block()
    share_sums = xwalk.groupby(xwalk.index)["parcel_block_share"].sum()
    assert np.allclose(share_sums.values, 1.0)


# --- Commercial total-stock roll-up ------------------------------------------

def _commercial_buildings():
    # commercial supply on BOTH base-year stock (h5_inputs) and new developer
    # rows -- the total-stock roll-up must count both, unlike build_block_supply
    # which drops h5_inputs.
    return pd.DataFrame({
        "parcel_id":            [1,           2,                 3,                 4],
        "job_spaces":           [5,           3,                 2,                 0],
        "non_residential_sqft": [2000,        100,               50,                0],
        "source":               ["h5_inputs", "developer_model", "developer_model", "h5_inputs"],
    })


def test_commercial_stock_includes_base_stock():
    out = build_block_commercial_stock(_parcel_block(), _commercial_buildings())
    # block A = p1 (h5 base, 5) + p2 (dev, 3); the base-year 5 must be counted
    assert out.loc["A", "job_spaces"] == 8
    assert out.loc["A", "non_residential_sqft"] == 2100   # 2000 + 100
    assert out.loc["B", "job_spaces"] == 2
    assert out.loc["B", "non_residential_sqft"] == 50


def test_commercial_stock_conservation():
    out = build_block_commercial_stock(_parcel_block(), _commercial_buildings())
    # block totals conserve the full building stock (base + new)
    assert out["job_spaces"].sum() == 10       # 5 + 3 + 2 + 0
    assert out["non_residential_sqft"].sum() == 2150  # 2000 + 100 + 50


def test_commercial_stock_apportions_split_parcel():
    # a parcel straddling two blocks splits its commercial supply by area share
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        index=pd.Index([1, 1], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id": [1], "job_spaces": [10], "non_residential_sqft": [1000],
        "source": ["h5_inputs"]})

    out = build_block_commercial_stock(parcel_block, buildings)
    assert out.loc["A", "job_spaces"] == 6.0             # 10 * 0.6
    assert out.loc["B", "job_spaces"] == 4.0             # 10 * 0.4
    assert out.loc["A", "non_residential_sqft"] == 600.0  # 1000 * 0.6
    # apportionment conserves the parcel totals across the split
    assert out["job_spaces"].sum() == 10.0
    assert out["non_residential_sqft"].sum() == 1000.0


# --- Residential price/rent roll-up (weighted mean) --------------------------

def _residential_units():
    # one row per unit, carrying the per-unit hedonic price/rent, with a
    # parcel_id column (the orca table adds this via building_id -> parcel_id).
    # parcels 1,2 -> block A; parcel 3 -> block B (see _parcel_block()).
    return pd.DataFrame({
        "parcel_id":              [1,     1,     2,     3],
        "unit_residential_price": [100.0, 300.0, 200.0, 500.0],
        "unit_residential_rent":  [1.0,   3.0,   2.0,   5.0],
    })


def test_residential_price_is_unit_weighted_mean():
    out = build_block_residential_price(_parcel_block(), _residential_units())
    # block A = parcels 1 (100, 300) + 2 (200): mean of 100,300,200 = 200
    assert out.loc["A", "unit_residential_price"] == 200.0
    assert out.loc["A", "unit_residential_rent"] == 2.0
    # block B = parcel 3 only
    assert out.loc["B", "unit_residential_price"] == 500.0
    assert out.loc["B", "unit_residential_rent"] == 5.0


def test_residential_price_apportions_split_parcel():
    # a parcel straddling two blocks splits its units by area share; the
    # weighted mean is unchanged when every unit shares one price, but the block
    # unit counts (denominator) split by share.
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [0.75, 0.25]},
        index=pd.Index([1, 1], name="parcel_id"))
    units = pd.DataFrame({
        "parcel_id":              [1,     1,     1,     1],
        "unit_residential_price": [100.0, 200.0, 300.0, 400.0],
        "unit_residential_rent":  [1.0,   2.0,   3.0,   4.0],
    })
    out = build_block_residential_price(parcel_block, units)
    # both blocks see the same four units, so the weighted mean is the plain
    # mean (250 / 2.5) regardless of the share split of the denominator.
    assert out.loc["A", "unit_residential_price"] == 250.0
    assert out.loc["B", "unit_residential_price"] == 250.0
    assert out.loc["A", "unit_residential_rent"] == 2.5
    assert out.loc["B", "unit_residential_rent"] == 2.5


# --- Non-residential rent roll-up (sqft-weighted mean) -----------------------

def test_nonres_rent_is_sqft_weighted_mean():
    # block A = parcel 1 (rent 10 over 100 sqft) + parcel 2 (rent 20 over 300
    # sqft): weighted mean = (10*100 + 20*300) / (100 + 300) = 7000/400 = 17.5
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "A"], "parcel_block_share": [1.0, 1.0]},
        index=pd.Index([1, 2], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id":            [1,     2],
        "non_residential_rent": [10.0,  20.0],
        "non_residential_sqft": [100.0, 300.0],
    })
    out = build_block_nonres_rent(parcel_block, buildings)
    assert out.loc["A", "non_residential_rent"] == 17.5


def test_nonres_rent_apportions_split_parcel():
    # a parcel straddling two blocks: the sqft-weighted mean of a single-price
    # parcel is that price in both blocks (share cancels in numerator/denominator).
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        index=pd.Index([1, 1], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id":            [1],
        "non_residential_rent": [25.0],
        "non_residential_sqft": [1000.0],
    })
    out = build_block_nonres_rent(parcel_block, buildings)
    assert out.loc["A", "non_residential_rent"] == 25.0
    assert out.loc["B", "non_residential_rent"] == 25.0


# --- Direct-run harness ------------------------------------------------------

if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("PASSED", name)
    print("All block-port tests passed.")
