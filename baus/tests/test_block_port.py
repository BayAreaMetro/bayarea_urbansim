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
    build_block_accessibility,
    build_block_placed_jobs,
    build_block_job_capacity,
    build_block_elcm_alternatives,
    build_block_unit_capacity,
    build_block_residential_price_by_tenure,
    build_block_submarket,
    build_block_hlcm_alternatives,
    ELCM_ACCESSIBILITY_COVARIATES,
    ELCM_ALTERNATIVE_COLUMNS,
    HLCM_ACCESSIBILITY_COVARIATES,
    HLCM_OWN_ALTERNATIVE_COLUMNS,
    HLCM_RENT_ALTERNATIVE_COLUMNS,
)
from baus.block_elcm import assign_job_block_geoid, render_block_jobs_to_buildings
from baus.block_hlcm import assign_household_block_geoid
from baus.block_developer import _dominant_block_for_parcels


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


# --- Block ELCM alternatives table (chunk 3.3) -------------------------------

def _elcm_buildings():
    # indexed by building_id (so placed jobs can map building_id -> parcel_id),
    # carrying supply, rent, and every accessibility covariate the ELCM spec
    # reads. parcels 1,2 -> block A; parcel 3 -> block B (see _parcel_block()).
    buildings = pd.DataFrame({
        "parcel_id":            [1,    1,    2,    3],
        "job_spaces":           [5,    3,    2,    4],
        "vacant_job_spaces":    [3,    3,    1,    3],
        "non_residential_sqft": [2000, 1000, 500,  400],
        "non_residential_rent": [10.0, 20.0, 40.0, 8.0],
    }, index=pd.Index([100, 101, 102, 103], name="building_id"))
    # give each accessibility covariate a distinct finite per-building value so
    # the spec model_expression evaluates without NaN/Inf.
    for offset, col in enumerate(ELCM_ACCESSIBILITY_COVARIATES):
        buildings[col] = [1.0 + offset, 2.0 + offset, 3.0 + offset, 4.0 + offset]
    return buildings


def _elcm_jobs():
    # placed jobs occupy buildings; -1 marks unplaced. building 100 holds 2 jobs,
    # 102 holds 1, 103 holds 1; one job is unplaced.
    return pd.DataFrame({"building_id": [100, 100, 102, 103, -1]})


def test_block_accessibility_is_area_weighted_mean():
    out = build_block_accessibility(_parcel_block(), _elcm_buildings(), ["office_1500"])
    # block A: parcel 1 mean(1,2)=1.5, parcel 2 =3 -> area mean (1.5+3)/2 = 2.25
    assert out.loc["A", "office_1500"] == 2.25
    assert out.loc["B", "office_1500"] == 4.0


def test_placed_jobs_rollup_excludes_unplaced():
    placed = build_block_placed_jobs(_parcel_block(), _elcm_buildings(), _elcm_jobs())
    assert placed.loc["A"] == 3   # parcel 1 (2 jobs) + parcel 2 (1 job)
    assert placed.loc["B"] == 1   # parcel 3


def test_block_job_capacity_sums_by_dominant_block():
    cap = build_block_job_capacity(_parcel_block(), _elcm_buildings())
    # buildings 100,101 (parcel 1) + 102 (parcel 2) -> block A; 103 (parcel 3) -> B
    assert cap.loc["A", "job_spaces"] == 10        # 5 + 3 + 2
    assert cap.loc["A", "vacant_job_spaces"] == 7  # 3 + 3 + 1
    assert cap.loc["B", "job_spaces"] == 4
    assert cap.loc["B", "vacant_job_spaces"] == 3


def test_block_elcm_vacant_job_spaces():
    alt = build_block_elcm_alternatives(_parcel_block(), _elcm_buildings())
    # supply = total-stock job_spaces per dominant block
    assert alt.loc["A", "job_spaces"] == 10   # 5 + 3 + 2
    assert alt.loc["B", "job_spaces"] == 4
    # vacancy = sum of building-level vacant_job_spaces per dominant block, integer
    assert alt.loc["A", "vacant_job_spaces"] == 7   # 3 + 3 + 1
    assert alt.loc["B", "vacant_job_spaces"] == 3
    assert alt["vacant_job_spaces"].dtype.kind == "i"


def test_block_elcm_alternatives_round_trips_model_expression():
    import patsy
    alt = build_block_elcm_alternatives(_parcel_block(), _elcm_buildings())
    # exposes exactly the spec covariates plus supply/vacancy, in order
    assert list(alt.columns) == ELCM_ALTERNATIVE_COLUMNS
    # the unchanged elcm.yaml model_expression must evaluate against the block table
    model_expression = (
        "np.log1p(non_residential_rent) + office_1500 + industrial_1500 + "
        "retail_1500 + jobs_45 + residential_units_1500 + residential_units_45 + "
        "ave_income_1500 + juris_ave_income + embarcadero + stanford")
    design = patsy.dmatrix(model_expression, alt, return_type="dataframe")
    assert np.isfinite(design.values).all()
    assert len(design) == len(alt)


def test_block_elcm_alternatives_fills_zero_commercial_rent():
    # A residential-only block (zero non-residential sqft) yields 0/0 = NaN rent,
    # which would trip lcm_simulate's check_nas. The alternatives builder must fill
    # it with 0 so the frame is NaN-free (the block is dropped by job_spaces > 0 at
    # predict time anyway, and np.log1p(0) == 0).
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [1.0, 1.0]},
        index=pd.Index([1, 2], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id":            [1, 2],
        "job_spaces":           [10, 0],
        "vacant_job_spaces":    [9, 0],
        "non_residential_sqft": [1000, 0],   # block B has no commercial stock
        "non_residential_rent": [25.0, 0.0],
    }, index=pd.Index([100, 101], name="building_id"))
    for col in ELCM_ACCESSIBILITY_COVARIATES:
        buildings[col] = [7.0, 3.0]

    alt = build_block_elcm_alternatives(parcel_block, buildings)
    assert alt.loc["B", "non_residential_rent"] == 0.0
    assert not alt["non_residential_rent"].isna().any()


def test_block_elcm_alternatives_assigns_split_parcel_to_dominant_block():
    # Under option B, block supply/vacancy roll up by DOMINANT block: a parcel that
    # straddles blocks A (0.6) and B (0.4) contributes its whole building capacity to
    # its dominant block A, and block B (no dominant-assigned building) is absent from
    # the alternatives -- so every block-placed job is renderable to a real building.
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        index=pd.Index([1, 1], name="parcel_id"))
    buildings = pd.DataFrame({
        "parcel_id":            [1],
        "job_spaces":           [10],
        "vacant_job_spaces":    [8],   # 10 job_spaces - 2 placed jobs
        "non_residential_sqft": [1000],
        "non_residential_rent": [25.0],
    }, index=pd.Index([100], name="building_id"))
    for col in ELCM_ACCESSIBILITY_COVARIATES:
        buildings[col] = [7.0]

    alt = build_block_elcm_alternatives(parcel_block, buildings)
    # whole parcel -> dominant block A; block B has no dominant building
    assert alt.loc["A", "job_spaces"] == 10
    assert alt.loc["A", "vacant_job_spaces"] == 8
    assert "B" not in alt.index
    # intensive covariates for A come from the (areal) roll-up of the parcel value
    assert np.isclose(alt.loc["A", "office_1500"], 7.0)
    assert np.isclose(alt.loc["A", "non_residential_rent"], 25.0)


# --- Block residential HLCM alternatives table (chunk 3.7) -------------------

def _hlcm_residential_units():
    # one row per unit, carrying tenure, deed-restricted flag, supply/vacancy, TAZ,
    # and the per-unit hedonic price/rent. parcels 1,2 -> block A; parcel 3 -> block
    # B (see _parcel_block()). Owner units sit on parcels 1,2 (block A); renter units
    # span block A (parcel 1) and block B (parcel 3). Parcel 1 also carries one
    # deed-restricted owner unit so the (block x deed_restricted) grain is exercised.
    return pd.DataFrame({
        "parcel_id":              [1,     1,     1,     2,     3,     3],
        "tenure":                 ["own", "own", "rent", "own", "rent", "rent"],
        "deed_restricted":        [0.0,   1.0,   0.0,   0.0,   0.0,   0.0],
        "num_units":              [1,     1,     1,     1,     1,     1],
        "vacant_units":           [1,     0,     1,     1,     1,     0],
        "zone_id":                [7,     7,     7,     7,     9,     9],
        "unit_residential_price": [100.0, 300.0, 0.0,   200.0, 0.0,   0.0],
        "unit_residential_rent":  [0.0,   0.0,   2.0,   0.0,   5.0,   3.0],
    })


def _hlcm_buildings():
    # accessibility covariates the HLCM specs read, one row per building, with a
    # parcel_id column. parcels 1,2 -> block A; parcel 3 -> block B. Each covariate
    # gets distinct finite per-parcel values so the model_expression evaluates.
    buildings = pd.DataFrame({"parcel_id": [1, 2, 3]},
                             index=pd.Index([100, 102, 103], name="building_id"))
    for offset, col in enumerate(HLCM_ACCESSIBILITY_COVARIATES):
        buildings[col] = [1.0 + offset, 3.0 + offset, 5.0 + offset]
    return buildings


def test_block_unit_capacity_sums_by_dominant_block_and_dr():
    cap = build_block_unit_capacity(_parcel_block(), _hlcm_residential_units(), "own")
    # owner units on parcels 1,2 -> block A. DR=0.0 group: units on p1 (vac 1) + p2
    # (vac 1) = 2 units, 2 vacant; DR=1.0 group: the one restricted p1 unit.
    assert cap.loc[("A", 0.0), "num_units"] == 2
    assert cap.loc[("A", 0.0), "vacant_units"] == 2
    assert cap.loc[("A", 1.0), "num_units"] == 1
    assert cap.loc[("A", 1.0), "vacant_units"] == 0
    assert cap["num_units"].dtype.kind == "i"
    # owner supply conserved: 3 owner units all on-crosswalk
    assert cap["num_units"].sum() == 3


def test_block_unit_capacity_renter_spans_two_blocks():
    cap = build_block_unit_capacity(_parcel_block(), _hlcm_residential_units(), "rent")
    # renter units: parcel 1 (block A, 1 unit) + parcel 3 (block B, 2 units)
    assert cap.loc[("A", 0.0), "num_units"] == 1
    assert cap.loc[("B", 0.0), "num_units"] == 2
    assert cap.loc[("B", 0.0), "vacant_units"] == 1   # one of the two p3 units vacant


def test_block_residential_price_by_tenure_is_unit_weighted():
    price = build_block_residential_price_by_tenure(
        _parcel_block(), _hlcm_residential_units(), "own", "unit_residential_price")
    # block A DR=0: owner prices 100 (p1) and 200 (p2) -> mean 150; DR=1: 300
    assert price.loc[("A", 0.0), "unit_residential_price"] == 150.0
    assert price.loc[("A", 1.0), "unit_residential_price"] == 300.0
    rent = build_block_residential_price_by_tenure(
        _parcel_block(), _hlcm_residential_units(), "rent", "unit_residential_rent")
    # block B DR=0: renter rents 5 and 3 -> mean 4
    assert rent.loc[("B", 0.0), "unit_residential_rent"] == 4.0


def test_block_submarket_is_majority_taz():
    submarket = build_block_submarket(_parcel_block(), _hlcm_residential_units())
    # all block A units sit in TAZ 7; both block B units in TAZ 9
    assert submarket.loc["A"] == 7
    assert submarket.loc["B"] == 9


def test_block_submarket_breaks_ties_by_smallest_zone():
    # a block split evenly between two TAZs picks the smaller zone id deterministically
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A"], "parcel_block_share": [1.0]},
        index=pd.Index([1], name="parcel_id"))
    units = pd.DataFrame({"parcel_id": [1, 1], "zone_id": [9, 5]})
    submarket = build_block_submarket(parcel_block, units)
    assert submarket.loc["A"] == 5


def test_block_hlcm_own_alternatives_columns_and_values():
    alt = build_block_hlcm_alternatives(
        _parcel_block(), _hlcm_residential_units(), _hlcm_buildings(), "own")
    assert list(alt.columns) == HLCM_OWN_ALTERNATIVE_COLUMNS
    keyed = alt.set_index(["block_geoid", "deed_restricted"])
    # block A, non-deed-restricted owner alternative
    row = keyed.loc[("A", False)]
    assert row["num_units"] == 2
    assert row["vacant_units"] == 2
    assert row["unit_residential_price"] == 150.0
    assert row["submarket_id"] == 7
    assert row["tenure"] == "own"
    # accessibility is the area-weighted mean of parcels 1,2: jobs_45 = mean(1,3) = 2
    assert np.isclose(row["jobs_45"], 2.0)
    # a distinct deed-restricted owner alternative exists for the same block
    assert ("A", True) in keyed.index
    assert keyed.loc[("A", True), "unit_residential_price"] == 300.0


def test_block_hlcm_rent_alternatives_columns_and_values():
    alt = build_block_hlcm_alternatives(
        _parcel_block(), _hlcm_residential_units(), _hlcm_buildings(), "rent")
    assert list(alt.columns) == HLCM_RENT_ALTERNATIVE_COLUMNS
    keyed = alt.set_index(["block_geoid", "deed_restricted"])
    row = keyed.loc[("B", False)]
    assert row["num_units"] == 2
    assert row["unit_residential_rent"] == 4.0
    assert row["submarket_id"] == 9
    assert row["tenure"] == "rent"
    # block B accessibility comes from parcel 3 only: jobs_45 = 5
    assert np.isclose(row["jobs_45"], 5.0)


def test_block_hlcm_alternatives_deed_restricted_is_bool():
    alt = build_block_hlcm_alternatives(
        _parcel_block(), _hlcm_residential_units(), _hlcm_buildings(), "own")
    assert alt["deed_restricted"].dtype == bool
    # owner block A appears once per deed_restricted value (True and False)
    block_a = alt[alt["block_geoid"] == "A"]
    assert set(block_a["deed_restricted"]) == {True, False}


def test_block_hlcm_alternatives_no_nans_and_int_counts():
    alt = build_block_hlcm_alternatives(
        _parcel_block(), _hlcm_residential_units(), _hlcm_buildings(), "own")
    assert not alt[HLCM_ACCESSIBILITY_COVARIATES].isna().any().any()
    assert alt["num_units"].dtype.kind == "i"
    assert alt["vacant_units"].dtype.kind == "i"
    assert alt["submarket_id"].notna().all()


def test_block_hlcm_owner_alternatives_round_trips_model_expression():
    import patsy
    alt = build_block_hlcm_alternatives(
        _parcel_block(), _hlcm_residential_units(), _hlcm_buildings(), "own")
    # the unchanged hlcm_owner.yaml model_expression must evaluate on the block table
    model_expression = (
        "jobs_45 + ave_income_1500 + np.log1p(unit_residential_price) + "
        "embarcadero + pacheights + stanford")
    design = patsy.dmatrix(model_expression, alt, return_type="dataframe")
    assert np.isfinite(design.values).all()
    assert len(design) == len(alt)


def test_block_hlcm_renter_alternatives_round_trips_model_expression():
    import patsy
    alt = build_block_hlcm_alternatives(
        _parcel_block(), _hlcm_residential_units(), _hlcm_buildings(), "rent")
    model_expression = (
        "jobs_45 + ave_income_1500 + np.log1p(unit_residential_rent) + "
        "embarcadero + pacheights + stanford")
    design = patsy.dmatrix(model_expression, alt, return_type="dataframe")
    assert np.isfinite(design.values).all()
    assert len(design) == len(alt)


def test_block_hlcm_alternatives_assigns_split_parcel_to_dominant_block():
    # option B: a parcel straddling blocks A (0.6) and B (0.4) contributes its whole
    # unit supply to its dominant block A; block B has no dominant-assigned owner unit
    # and is absent -- so every block-placed household is renderable to a real unit.
    parcel_block = pd.DataFrame(
        {"block_geoid": ["A", "B"], "parcel_block_share": [0.6, 0.4]},
        index=pd.Index([1, 1], name="parcel_id"))
    units = pd.DataFrame({
        "parcel_id":              [1,     1],
        "tenure":                 ["own", "own"],
        "deed_restricted":        [0.0,   0.0],
        "num_units":              [1,     1],
        "vacant_units":           [1,     1],
        "zone_id":                [7,     7],
        "unit_residential_price": [100.0, 200.0],
        "unit_residential_rent":  [0.0,   0.0],
    })
    buildings = pd.DataFrame({"parcel_id": [1]},
                             index=pd.Index([100], name="building_id"))
    for col in HLCM_ACCESSIBILITY_COVARIATES:
        buildings[col] = [4.0]

    alt = build_block_hlcm_alternatives(parcel_block, units, buildings, "own")
    keyed = alt.set_index(["block_geoid", "deed_restricted"])
    # whole parcel -> dominant block A; block B absent
    assert keyed.loc[("A", False), "num_units"] == 2
    assert "B" not in alt["block_geoid"].values
    assert np.isclose(keyed.loc[("A", False), "jobs_45"], 4.0)


# --- Job -> block key assignment (chunk 3.4) ---------------------------------

def test_assign_job_block_geoid_movers_and_placed():
    # block_geoid is int64 (parcels_block now loads it numeric); placed jobs take
    # their building's parcel's dominant block, unplaced jobs take the -1 sentinel.
    parcel_block = pd.DataFrame(
        {"block_geoid": [60750611012023, 60014001001000],
         "parcel_block_share": [1.0, 1.0]},
        index=pd.Index([7, 9], name="parcel_id"))
    dominant_block = _dominant_block_for_parcels(parcel_block)
    buildings = pd.DataFrame(
        {"parcel_id": [7, 9]},
        index=pd.Index([100, 102], name="building_id"))
    jobs = pd.DataFrame({"building_id": [100, 102, -1]})
    out = assign_job_block_geoid(jobs, buildings, dominant_block)
    assert out.tolist() == [60750611012023, 60014001001000, -1]
    assert out.dtype == np.int64


def test_assign_job_block_geoid_uses_dominant_block():
    # parcel 7 straddles two blocks; the larger-share (0.7) block is chosen.
    parcel_block = pd.DataFrame(
        {"block_geoid": [60750611012023, 60750611012099],
         "parcel_block_share": [0.7, 0.3]},
        index=pd.Index([7, 7], name="parcel_id"))
    dominant_block = _dominant_block_for_parcels(parcel_block)
    buildings = pd.DataFrame(
        {"parcel_id": [7]}, index=pd.Index([100], name="building_id"))
    jobs = pd.DataFrame({"building_id": [100, -1]})
    out = assign_job_block_geoid(jobs, buildings, dominant_block)
    assert out.tolist() == [60750611012023, -1]
    assert out.dtype == np.int64


def test_assign_job_block_geoid_off_crosswalk_placed_job_gets_minus_two():
    # A placed job whose parcel is absent from the crosswalk has no dominant block;
    # it must get the distinct -2 (non-mover) sentinel, not -1 (which would make it
    # a mover), and the int64 cast must still succeed (no NaN survives).
    parcel_block = pd.DataFrame(
        {"block_geoid": [60750611012023], "parcel_block_share": [1.0]},
        index=pd.Index([7], name="parcel_id"))
    dominant_block = _dominant_block_for_parcels(parcel_block)
    # building 102 sits on parcel 9, which is NOT in the crosswalk.
    buildings = pd.DataFrame(
        {"parcel_id": [7, 9]},
        index=pd.Index([100, 102], name="building_id"))
    jobs = pd.DataFrame({"building_id": [100, 102, -1]})
    out = assign_job_block_geoid(jobs, buildings, dominant_block)
    assert out.tolist() == [60750611012023, -2, -1]
    assert out.dtype == np.int64


def test_assign_household_block_geoid_movers_and_placed():
    # Mirrors the job-side helper: placed households take their building's parcel's
    # dominant block, unplaced households (building_id == -1) take the -1 sentinel.
    parcel_block = pd.DataFrame(
        {"block_geoid": [60750611012023, 60014001001000],
         "parcel_block_share": [1.0, 1.0]},
        index=pd.Index([7, 9], name="parcel_id"))
    dominant_block = _dominant_block_for_parcels(parcel_block)
    buildings = pd.DataFrame(
        {"parcel_id": [7, 9]},
        index=pd.Index([100, 102], name="building_id"))
    households = pd.DataFrame({"building_id": [100, 102, -1]})
    out = assign_household_block_geoid(households, buildings, dominant_block)
    assert out.tolist() == [60750611012023, 60014001001000, -1]
    assert out.dtype == np.int64


def test_assign_household_block_geoid_uses_dominant_block():
    # parcel 7 straddles two blocks; the larger-share (0.7) block is chosen.
    parcel_block = pd.DataFrame(
        {"block_geoid": [60750611012023, 60750611012099],
         "parcel_block_share": [0.7, 0.3]},
        index=pd.Index([7, 7], name="parcel_id"))
    dominant_block = _dominant_block_for_parcels(parcel_block)
    buildings = pd.DataFrame(
        {"parcel_id": [7]}, index=pd.Index([100], name="building_id"))
    households = pd.DataFrame({"building_id": [100, -1]})
    out = assign_household_block_geoid(households, buildings, dominant_block)
    assert out.tolist() == [60750611012023, -1]
    assert out.dtype == np.int64


def test_assign_household_block_geoid_off_crosswalk_placed_gets_minus_two():
    # A placed household whose parcel is absent from the crosswalk has no dominant
    # block; it must get the distinct -2 (non-mover) sentinel, not -1 (which would
    # make it a mover), and the int64 cast must still succeed (no NaN survives).
    parcel_block = pd.DataFrame(
        {"block_geoid": [60750611012023], "parcel_block_share": [1.0]},
        index=pd.Index([7], name="parcel_id"))
    dominant_block = _dominant_block_for_parcels(parcel_block)
    # building 102 sits on parcel 9, which is NOT in the crosswalk.
    buildings = pd.DataFrame(
        {"parcel_id": [7, 9]},
        index=pd.Index([100, 102], name="building_id"))
    households = pd.DataFrame({"building_id": [100, 102, -1]})
    out = assign_household_block_geoid(households, buildings, dominant_block)
    assert out.tolist() == [60750611012023, -2, -1]
    assert out.dtype == np.int64


def test_block_geoid_zfill_reconstructs_15_digit_string():
    # block_supply_summary re-pads the int64 GEOID to the canonical 15-char string
    # so its CSV output is byte-identical to the string-keyed baseline.
    idx = pd.Index([60750611012023], name="block_geoid")
    padded = idx.map(lambda geoid: str(geoid).zfill(15))
    assert list(padded) == ["060750611012023"]


# --- Block choice -> building rendering bridge (chunk 3.5) --------------------

def test_render_block_jobs_happy_path_fills_within_capacity():
    # One block (55) with two buildings: 3 + 2 = 5 vacant slots, 4 rendering jobs.
    # All 4 jobs land in that block, no building exceeds its vacant job spaces.
    dominant_block = pd.Series(
        [55, 55], index=pd.Index([7, 8], name="parcel_id"))
    buildings = pd.DataFrame(
        {"parcel_id": [7, 8], "vacant_job_spaces": [3, 2]},
        index=pd.Index([100, 101], name="building_id"))
    jobs = pd.DataFrame({"building_id": [-1, -1, -1, -1],
                         "block_geoid": [55, 55, 55, 55]})
    out = render_block_jobs_to_buildings(jobs, buildings, dominant_block)
    assert len(out) == 4
    assert out.dtype == np.int64
    counts = out.value_counts()
    assert counts.get(100, 0) <= 3
    assert counts.get(101, 0) <= 2
    assert set(out.unique()) <= {100, 101}


def test_render_block_jobs_over_capacity_leaves_tail_unplaced():
    # Block 55 has only 2 vacant slots but 3 rendering jobs; 2 render, 1 stays -1
    # (omitted from the update Series).
    dominant_block = pd.Series([55], index=pd.Index([7], name="parcel_id"))
    buildings = pd.DataFrame(
        {"parcel_id": [7], "vacant_job_spaces": [2]},
        index=pd.Index([100], name="building_id"))
    jobs = pd.DataFrame({"building_id": [-1, -1, -1],
                         "block_geoid": [55, 55, 55]})
    out = render_block_jobs_to_buildings(jobs, buildings, dominant_block)
    assert len(out) == 2
    assert (out == 100).all()


def test_render_block_jobs_ignores_placed_and_blockless_jobs():
    # Placed jobs (building_id != -1) and block-less unplaced jobs (block_geoid ==
    # -1) never appear in the update Series.
    dominant_block = pd.Series([55], index=pd.Index([7], name="parcel_id"))
    buildings = pd.DataFrame(
        {"parcel_id": [7], "vacant_job_spaces": [5]},
        index=pd.Index([100], name="building_id"))
    jobs = pd.DataFrame(
        {"building_id": [999, -1, -1], "block_geoid": [55, -1, 55]},
        index=pd.Index([0, 1, 2], name="job_id"))
    out = render_block_jobs_to_buildings(jobs, buildings, dominant_block)
    assert out.index.tolist() == [2]
    assert out.tolist() == [100]


def test_render_block_jobs_is_deterministic():
    # Identical inputs yield identical assignments (stable order, no RNG).
    dominant_block = pd.Series(
        [55, 55], index=pd.Index([7, 8], name="parcel_id"))
    buildings = pd.DataFrame(
        {"parcel_id": [7, 8], "vacant_job_spaces": [2, 2]},
        index=pd.Index([100, 101], name="building_id"))
    jobs = pd.DataFrame({"building_id": [-1, -1, -1, -1],
                         "block_geoid": [55, 55, 55, 55]})
    first = render_block_jobs_to_buildings(jobs, buildings, dominant_block)
    second = render_block_jobs_to_buildings(jobs, buildings, dominant_block)
    pd.testing.assert_series_equal(first, second)
    # buildings fill in ascending building_id order: 100, 100, 101, 101.
    assert first.tolist() == [100, 100, 101, 101]


# --- Direct-run harness ------------------------------------------------------

if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("PASSED", name)
    print("All block-port tests passed.")
