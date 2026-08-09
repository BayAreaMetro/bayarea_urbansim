"""Block-level household location choice (HLCM) for the BAUS block port.

This module wires the existing HLCM specs (``configs/location_choice/hlcm_owner.yaml``,
``hlcm_renter.yaml`` and their lowincome variants, reused unchanged) onto the
census-block alternatives tables built in Phase-3 chunk 3.7
(``baus.block_supply.block_own_alternatives`` / ``block_rent_alternatives``). Instead
of choosing among *residential units*, unplaced households choose among *blocks*:
``utils.lcm_simulate`` runs with ``out_fname='block_alt_id'``,
``supply_fname='num_units'`` and ``vacant_fname='vacant_units'`` pointed at the block
table, so the fitted HLCM coefficients score block covariates directly (Option B --
covariates materialized on the block table, no ``join_tbls`` broadcast). ``block_geoid``
is a non-unique column on the block x deed_restricted alternatives table, so the choice
is made on the unique ``block_alt_id`` index (which ``lcm_simulate`` recovers via
``reset_index``) and the chosen alternative is then mapped back to its ``block_geoid``.

Choosers are segmented by tenure exactly as the parcel HLCM does
(``correct_alternative_filters_sample`` builds ``own_hh`` / ``rent_hh``), and each
tenure's choosers are scored against that tenure's block table (owners against
``block_own_alternatives``, renters against ``block_rent_alternatives``). Household
``tenure`` is a fixed PUMS attribute that is never updated during the simulation, so a
mover still carries it. Deed-restricted awareness comes from each spec's
``alts_predict_filters`` acting on the block table's ``deed_restricted`` column
(chunk 3.7), so the choice key written back to households is a single
``block_geoid`` -- not a composite ``(block, deed_restricted)`` key.

``utils.lcm_simulate`` identifies movers as ``choosers[out_fname] == -1`` and expands
vacancy by repeating the alternatives' index, so the choice key must be a numeric
column with a ``-1`` unplaced sentinel. Each step therefore seeds an int64
``block_alt_id`` column onto ``households`` (``-1`` for movers, ``0`` for placed
households), runs the choice, then maps each mover's chosen ``block_alt_id`` back to
its ``block_geoid`` (int64; the 15-digit GEOID fits int64). Placed households keep the
dominant block of their building's parcel, and movers the choice left unplaced keep
the ``-1`` sentinel.

After the block choice, each step *renders* it down to a concrete ``unit_id``: a thin,
behavior-free bridge (``render_block_households_to_units``) assigns each newly
block-placed household a vacant residential unit of its tenure (and, for the low-income
steps, a deed-restricted unit) in the chosen block, filling candidate units
deterministically. ``building_id`` is not written here -- the existing
``baus.ual.reconcile_placed_households`` step resolves it from ``unit_id`` -- so tenure
assignment, relocation, and every household-geography variable/summary keep working
unchanged. Supply/demand equilibration is turned on
(``enable_supply_correction=price_settings.get(...)``) at the TAZ submarket grain
(``submarket_col='submarket_id'``, majority-TAZ from chunk 3.7). Because the block
alternatives table is ``cache=False``, the price the equilibration mutates on it is
ephemeral -- exactly like the parcel regular/low-income HLCMs mutating their
``own_units`` copy; the persisted TAZ-level price clearing comes from the retained
parcel ``*_no_unplaced`` steps, which equilibrate on the real ``residential_units``.

Wiring the ``hlcm_geography`` toggle and swapping these steps in for the parcel HLCMs
is Chunk 3.10. The steps are registered but not yet in the annual model list, and the
parcel HLCMs (``baus/ual.py``) are left untouched for A/B comparison, so registering
this module changes no model behavior.

See Also:
    baus.block_supply.block_own_alternatives: the owner block alternatives table these
        steps choose among.
    baus.block_supply.block_rent_alternatives: the renter block alternatives table.
    baus.ual.hlcm_owner_simulate: the parcel-native HLCM these steps mirror and run
        beside for A/B comparison.
"""

import orca
import pandas as pd
from urbansim_defaults import utils

from baus.block_developer import _dominant_block_for_parcels
from baus.ual import correct_alternative_filters_sample

__all__ = [
    # Pure helpers (unit-testable, no orca)
    'assign_household_block_geoid',
    'render_block_households_to_units',
    # Orca-table plumbing (block choice write-back)
    'update_household_block_geoids',
    # Live orca steps (block HLCM choice, tenure-matched)
    'block_hlcm_owner_simulate',
    'block_hlcm_renter_simulate',
    'block_hlcm_owner_lowincome_simulate',
    'block_hlcm_renter_lowincome_simulate',
]


def assign_household_block_geoid(households, buildings, dominant_block):
    """Assigns each household the int64 block key ``utils.lcm_simulate`` chooses from.

    Unplaced households (``building_id == -1``) receive the ``-1`` sentinel that
    ``lcm_simulate`` uses to identify movers; placed households receive the dominant
    (largest-area-share) block of their building's parcel. A small share of parcels
    are absent from the areal ``parcels_block`` crosswalk (e.g. parcels dropped for
    void geometry), so a placed household on such a parcel has no dominant block; it
    receives a distinct ``-2`` sentinel so it stays a non-mover (``lcm_simulate`` only
    moves ``-1``) and is excluded from block rendering, while the block supply/vacancy
    roll-ups -- which inner-join the same crosswalk -- already omit it on both the
    supply and occupancy sides. The result is an int64 column so it can serve as the
    numeric alternatives key.

    This mirrors ``baus.block_elcm.assign_job_block_geoid``; household relocation sets
    both ``unit_id`` and ``building_id`` to ``-1`` together, so the ``building_id``
    mover flag is equivalent to the HLCM's canonical ``unit_id == -1`` flag.

    Args:
        households: DataFrame with a ``building_id`` column (one row per household); an
            unplaced household is flagged by ``building_id == -1``.
        buildings: DataFrame indexed by ``building_id`` with a ``parcel_id`` column.
        dominant_block: Series indexed by ``parcel_id`` giving each parcel's dominant
            int64 ``block_geoid`` (see ``_dominant_block_for_parcels``).

    Returns:
        An int64 Series aligned to ``households`` with ``-1`` for unplaced households,
        ``-2`` for placed households whose parcel is absent from the crosswalk, and the
        placed household's parcel's dominant block otherwise.

    Example:
        >>> import pandas as pd
        >>> households = pd.DataFrame({"building_id": [100, -1]})
        >>> buildings = pd.DataFrame(
        ...     {"parcel_id": [7]}, index=pd.Index([100], name="building_id"))
        >>> dominant_block = pd.Series(
        ...     [60750611012023], index=pd.Index([7], name="parcel_id"))
        >>> assign_household_block_geoid(
        ...     households, buildings, dominant_block).tolist()
        [60750611012023, -1]

    See Also:
        block_hlcm_owner_simulate: an orca step that writes this column onto
            ``households`` and runs the block HLCM.
        baus.block_elcm.assign_job_block_geoid: the employment-side counterpart.
    """
    household_parcel = households["building_id"].map(buildings["parcel_id"])
    household_block = household_parcel.map(dominant_block)
    is_placed = households["building_id"] != -1
    household_block = household_block.where(is_placed, other=-1)
    household_block = household_block.where(household_block.notna(), other=-2)
    return household_block.astype("int64")


def render_block_households_to_units(choosers, unit_alternatives, deed_restricted_only):
    """Renders each block-placed household down to a concrete vacant unit in its block.

    The block HLCM leaves a newly placed mover with a chosen ``block_geoid`` but still
    ``unit_id == -1``. This thin, behavior-free bridge assigns each such household a
    vacant residential unit of its tenure inside its chosen block, filling candidate
    units deterministically (stable ``unit_id`` order, no RNG) so no unit is ever
    assigned to more than one household. Unlike the employment-side counterpart, one
    residential unit holds exactly one household, so each vacant unit contributes a
    single slot (no vacancy expansion). Households whose chosen block runs out of
    vacant units keep ``unit_id == -1`` -- the normal unplaced tail the retained parcel
    ``*_no_unplaced`` steps mop up -- so the returned Series covers only the households
    actually rendered. ``building_id`` is intentionally left to
    ``baus.ual.reconcile_placed_households``.

    Low-income steps place Q1 households into deed-restricted stock, so
    ``deed_restricted_only`` restricts candidate units to ``deed_restricted == True``,
    mirroring ``hlcm_owner_lowincome.yaml`` / ``hlcm_renter_lowincome.yaml``
    ``alts_predict_filters``; the regular steps leave it ``False`` and, like
    ``hlcm_owner.yaml`` / ``hlcm_renter.yaml`` (``alts_predict_filters`` on tenure
    only), may fill either deed-restricted or market-rate units. A unit's block is its
    building's parcel's dominant block, so a block's candidate units are exactly those
    its block-HLCM vacancy was rolled up from (chunk 3.7); the per-block candidate pool
    therefore equals the per-block choice capacity and the areal seam leaves no
    household unrenderable. Off-crosswalk units carry a ``NaN`` block and are naturally
    excluded (a groupby drops the ``NaN`` key), so they never match a chosen block.

    Args:
        choosers: DataFrame indexed by household id with an integer ``unit_id`` column
            and an int64 ``block_geoid`` column (already tenure-filtered to this step's
            segment). A household to render has ``unit_id == -1`` and a real
            (``>= 0``) ``block_geoid``.
        unit_alternatives: DataFrame indexed by ``unit_id`` with a ``block_geoid``
            column, a ``deed_restricted`` column (float 1.0/0.0 or boolean), and an
            integer ``vacant_units`` column (the tenure's residential units). A unit is
            a candidate slot when ``vacant_units > 0``.
        deed_restricted_only: If True, restrict candidate units to
            ``deed_restricted == True`` (the low-income steps); if False, any vacant
            unit of the tenure is a candidate (the regular steps).

    Returns:
        An int64 Series indexed by household id holding the assigned ``unit_id`` for
        each rendered household only; households left unplaced are omitted.

    Example:
        >>> import pandas as pd
        >>> unit_alternatives = pd.DataFrame(
        ...     {"block_geoid": [55], "deed_restricted": [0.0],
        ...      "vacant_units": [1]},
        ...     index=pd.Index([900], name="unit_id"))
        >>> choosers = pd.DataFrame(
        ...     {"unit_id": [-1], "block_geoid": [55]},
        ...     index=pd.Index([10], name="household_id"))
        >>> render_block_households_to_units(
        ...     choosers, unit_alternatives, False).tolist()
        [900]

    See Also:
        _block_hlcm_simulate: the core that applies these ``unit_id`` assignments.
        baus.block_elcm.render_block_jobs_to_buildings: the employment-side counterpart.
    """
    slots = unit_alternatives[unit_alternatives["vacant_units"] > 0]
    if deed_restricted_only:
        slots = slots[slots["deed_restricted"] == True]
    slots = slots.reset_index()
    unit_id_col = slots.columns[0]
    slots = slots.sort_values(unit_id_col)
    slots["_slot_rank"] = slots.groupby("block_geoid").cumcount()

    to_render = choosers[(choosers["unit_id"] == -1) & (choosers["block_geoid"] >= 0)]
    to_render = to_render.sort_index().reset_index()
    household_id_col = to_render.columns[0]
    to_render["_slot_rank"] = to_render.groupby("block_geoid").cumcount()

    matched = to_render[[household_id_col, "block_geoid", "_slot_rank"]].merge(
        slots[[unit_id_col, "block_geoid", "_slot_rank"]],
        on=["block_geoid", "_slot_rank"], how="inner")
    return pd.Series(matched[unit_id_col].values,
                     index=matched[household_id_col].values,
                     name="unit_id").astype("int64")


def update_household_block_geoids(households, tenure):
    """Copies the block a tenure segment just chose back onto the households table.

    ``utils.lcm_simulate`` writes each mover's chosen ``block_geoid`` onto the
    tenure-segmented choosers table (``own_hh`` or ``rent_hh``) that
    ``correct_alternative_filters_sample`` registered, not onto the full
    ``households`` table. This mirrors ``baus.ual.update_unit_ids`` for the block key:
    it pulls the updated ``block_geoid`` values from the tenure table and writes them
    back onto the households of that tenure, leaving other households unchanged.

    Args:
        households: The orca ``households`` table.
        tenure: The tenure segment just simulated, ``"own"`` or ``"rent"``.

    Returns:
        None. The ``block_geoid`` column is updated in place on the ``households``
        table.

    See Also:
        baus.ual.update_unit_ids: the parcel-path analogue that copies back ``unit_id``.
    """
    block_geoids = households.to_frame(["block_geoid"])
    updated = orca.get_table(tenure + "_hh").to_frame(["block_geoid"])
    block_geoids.loc[block_geoids.index.isin(updated.index),
                     "block_geoid"] = updated["block_geoid"]
    households.update_col_from_series(
        "block_geoid", block_geoids.block_geoid, cast=True)


def _block_hlcm_simulate(households, residential_units, buildings, parcels_block,
                         block_alternatives, price_settings, yaml_name, tenure,
                         equilibration_name, deed_restricted_only):
    """Runs one tenure segment's block HLCM choice (shared core of the four steps).

    Writes an int64 ``block_geoid`` column onto ``households`` (``-1`` for unplaced
    households, the dominant block otherwise), segments choosers by tenure the same
    way the parcel HLCM does (``correct_alternative_filters_sample`` builds ``own_hh``
    / ``rent_hh``), then runs ``utils.lcm_simulate`` with the fitted spec against the
    tenure's block alternatives table so each mover chooses a block. The chosen
    ``block_geoid`` is copied back onto ``households`` and rendered down to a concrete
    ``unit_id`` via ``render_block_households_to_units`` (``building_id`` is left to
    ``baus.ual.reconcile_placed_households``).

    Supply/demand equilibration is enabled
    (``enable_supply_correction=price_settings.get(equilibration_name, None)``) and
    clears at the TAZ submarket grain (chunk 3.7). Because ``block_alternatives`` is
    ``cache=False`` its mutated price is ephemeral (matching the parcel regular /
    low-income HLCMs on their ``own_units`` copy); persisted TAZ-level price clearing
    comes from the retained parcel ``*_no_unplaced`` steps. Covariates are materialized
    on the block table (chunk 3.7), so no ``join_tbls`` broadcast is passed.

    Args:
        households: The orca ``households`` table (choosers); unplaced households have
            ``building_id == -1``.
        residential_units: The orca ``residential_units`` table, used by
            ``correct_alternative_filters_sample`` to segment choosers by tenure.
        buildings: The orca ``buildings`` table, used to map placed households to
            parcels.
        parcels_block: The areal parcel-to-block crosswalk table, used to derive each
            parcel's dominant block.
        block_alternatives: The tenure's block-indexed HLCM alternatives table (chunk
            3.7) with ``num_units`` supply and integer ``vacant_units`` vacancy.
        price_settings: The ``price_settings`` injectable
            (``configs/hedonics/price_settings.yaml``) whose ``equilibration_name``
            entry configures ``lcm_simulate``'s supply correction.
        yaml_name: The HLCM spec file name (e.g. ``"hlcm_owner.yaml"``), read from
            ``configs/location_choice``.
        tenure: The tenure segment to simulate, ``"own"`` or ``"rent"``.
        equilibration_name: The ``price_settings`` key selecting the supply-correction
            config (``"price_equilibration"`` for owners, ``"rent_equilibration"`` for
            renters).
        deed_restricted_only: If True, restrict rendered units to deed-restricted stock
            (the low-income steps); if False, any vacant unit of the tenure is eligible.

    Returns:
        None. Mutates the ``households`` table's ``block_geoid`` and ``unit_id``
        columns in place.

    See Also:
        assign_household_block_geoid: the pure helper that builds the initial int64 key.
        render_block_households_to_units: the pure helper that renders block to unit.
        baus.ual.hlcm_simulate: the parcel-path core this mirrors.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    dominant_block = _dominant_block_for_parcels(parcel_block)

    households_df = households.to_frame(["building_id"])
    buildings_df = buildings.to_frame(["parcel_id"])
    household_block_geoid = assign_household_block_geoid(
        households_df, buildings_df, dominant_block)
    households.update_col("block_geoid", household_block_geoid)

    # Segment choosers by tenure exactly as the parcel HLCM does (own_hh / rent_hh).
    # lcm_simulate requires its out_fname to be the alternatives' *index* (it recovers
    # the key via reset_index and reads it back off the chosen rows). block_geoid is
    # only a column on the block x deed_restricted alternatives table and is non-unique
    # across the deed_restricted split, so it cannot be the index; we choose on the
    # unique block_alt_id index and map the chosen alternative back to its block_geoid.
    # A -1 sentinel marks movers exactly as the block_geoid -1 sentinel does.
    households.update_col(
        "block_alt_id", -(households_df["building_id"] == -1).astype("int64"))
    correct_alternative_filters_sample(residential_units, households, tenure)

    utils.lcm_simulate(cfg="location_choice/" + yaml_name,
                       choosers=orca.get_table(tenure + "_hh"),
                       buildings=block_alternatives,
                       join_tbls=[],
                       out_fname="block_alt_id",
                       supply_fname="num_units",
                       vacant_fname="vacant_units",
                       enable_supply_correction=price_settings.get(
                           equilibration_name, None),
                       cast=True)

    # Map each mover's chosen alternative back to its block_geoid and write it onto
    # households. Placed households keep the dominant block assigned above; movers the
    # choice left unplaced (block_alt_id == -1) keep the -1 sentinel.
    alt_block_geoid = block_alternatives.to_frame(["block_geoid"])["block_geoid"]
    chosen_alt = orca.get_table(tenure + "_hh").to_frame(
        ["block_alt_id", "building_id"])
    mover_alt = chosen_alt.loc[chosen_alt["building_id"] == -1, "block_alt_id"]
    mover_block = mover_alt.map(alt_block_geoid)
    mover_block = mover_block.where(mover_block.notna(), other=-1).astype("int64")
    households.update_col_from_series("block_geoid", mover_block, cast=True)

    # Render each block choice down to a concrete vacant unit in that block. The
    # tenure's residential units (own_units / rent_units) carry a step-start
    # vacant_units snapshot that is still valid here because this step has not set any
    # unit_id; building_id is left to reconcile_placed_households.
    unit_alternatives = orca.get_table(tenure + "_units").to_frame(
        ["building_id", "deed_restricted", "vacant_units"])
    unit_alternatives["block_geoid"] = unit_alternatives["building_id"].map(
        buildings_df["parcel_id"]).map(dominant_block)
    tenure_index = orca.get_table(tenure + "_hh").to_frame(["unit_id"]).index
    choosers = households.to_frame(["unit_id", "block_geoid"]).loc[tenure_index]
    unit_updates = render_block_households_to_units(
        choosers, unit_alternatives, deed_restricted_only)
    households.update_col_from_series("unit_id", unit_updates, cast=True)


@orca.step()
def block_hlcm_owner_simulate(households, residential_units, buildings,
                              parcels_block, block_own_alternatives, price_settings):
    """Places unplaced owner households into census blocks (block HLCM choice).

    Runs the fitted owner HLCM spec (``hlcm_owner.yaml``) against the owner block
    alternatives table so each unplaced owner household chooses a ``block_geoid``, then
    renders that choice to a vacant owner ``unit_id`` in the chosen block and clears
    prices via owner supply/demand equilibration (``price_equilibration``).
    ``building_id`` is resolved downstream by ``reconcile_placed_households``.

    Args:
        households: The orca ``households`` table (choosers).
        residential_units: The orca ``residential_units`` table (tenure segmentation).
        buildings: The orca ``buildings`` table (placed-household parcel lookup).
        parcels_block: The areal parcel-to-block crosswalk table.
        block_own_alternatives: The owner block alternatives table (chunk 3.7).
        price_settings: The ``price_settings`` injectable configuring equilibration.

    Returns:
        None. Mutates the ``households`` table's ``block_geoid`` and ``unit_id``
        columns in place.

    See Also:
        _block_hlcm_simulate: the shared core these four steps delegate to.
        baus.ual.hlcm_owner_simulate: the parcel-native owner HLCM this mirrors.
    """
    _block_hlcm_simulate(households, residential_units, buildings, parcels_block,
                         block_own_alternatives, price_settings, "hlcm_owner.yaml",
                         "own", "price_equilibration", False)


@orca.step()
def block_hlcm_renter_simulate(households, residential_units, buildings,
                               parcels_block, block_rent_alternatives, price_settings):
    """Places unplaced renter households into census blocks (block HLCM choice).

    Runs the fitted renter HLCM spec (``hlcm_renter.yaml``) against the renter block
    alternatives table so each unplaced renter household chooses a ``block_geoid``, then
    renders that choice to a vacant renter ``unit_id`` in the chosen block and clears
    prices via renter supply/demand equilibration (``rent_equilibration``).
    ``building_id`` is resolved downstream by ``reconcile_placed_households``.

    Args:
        households: The orca ``households`` table (choosers).
        residential_units: The orca ``residential_units`` table (tenure segmentation).
        buildings: The orca ``buildings`` table (placed-household parcel lookup).
        parcels_block: The areal parcel-to-block crosswalk table.
        block_rent_alternatives: The renter block alternatives table (chunk 3.7).
        price_settings: The ``price_settings`` injectable configuring equilibration.

    Returns:
        None. Mutates the ``households`` table's ``block_geoid`` and ``unit_id``
        columns in place.

    See Also:
        _block_hlcm_simulate: the shared core these four steps delegate to.
        baus.ual.hlcm_renter_simulate: the parcel-native renter HLCM this mirrors.
    """
    _block_hlcm_simulate(households, residential_units, buildings, parcels_block,
                         block_rent_alternatives, price_settings, "hlcm_renter.yaml",
                         "rent", "rent_equilibration", False)


@orca.step()
def block_hlcm_owner_lowincome_simulate(households, residential_units, buildings,
                                        parcels_block, block_own_alternatives,
                                        price_settings):
    """Places unplaced low-income owner households into census blocks (block HLCM).

    Runs the fitted low-income owner HLCM spec (``hlcm_owner_lowincome.yaml``), whose
    ``alts_predict_filters`` restrict the owner block alternatives to deed-restricted
    stock, so each unplaced low-income owner household chooses a ``block_geoid``, then
    renders that choice to a vacant deed-restricted owner ``unit_id`` in the chosen
    block and clears prices via owner supply/demand equilibration
    (``price_equilibration``). ``building_id`` is resolved downstream by
    ``reconcile_placed_households``.

    Args:
        households: The orca ``households`` table (choosers).
        residential_units: The orca ``residential_units`` table (tenure segmentation).
        buildings: The orca ``buildings`` table (placed-household parcel lookup).
        parcels_block: The areal parcel-to-block crosswalk table.
        block_own_alternatives: The owner block alternatives table (chunk 3.7).
        price_settings: The ``price_settings`` injectable configuring equilibration.

    Returns:
        None. Mutates the ``households`` table's ``block_geoid`` and ``unit_id``
        columns in place.

    See Also:
        _block_hlcm_simulate: the shared core these four steps delegate to.
        baus.ual.hlcm_owner_lowincome_simulate: the parcel-native step this mirrors.
    """
    _block_hlcm_simulate(households, residential_units, buildings, parcels_block,
                         block_own_alternatives, price_settings,
                         "hlcm_owner_lowincome.yaml", "own", "price_equilibration",
                         True)


@orca.step()
def block_hlcm_renter_lowincome_simulate(households, residential_units, buildings,
                                         parcels_block, block_rent_alternatives,
                                         price_settings):
    """Places unplaced low-income renter households into census blocks (block HLCM).

    Runs the fitted low-income renter HLCM spec (``hlcm_renter_lowincome.yaml``), whose
    ``alts_predict_filters`` restrict the renter block alternatives to deed-restricted
    stock, so each unplaced low-income renter household chooses a ``block_geoid``, then
    renders that choice to a vacant deed-restricted renter ``unit_id`` in the chosen
    block and clears prices via renter supply/demand equilibration
    (``rent_equilibration``). ``building_id`` is resolved downstream by
    ``reconcile_placed_households``.

    Args:
        households: The orca ``households`` table (choosers).
        residential_units: The orca ``residential_units`` table (tenure segmentation).
        buildings: The orca ``buildings`` table (placed-household parcel lookup).
        parcels_block: The areal parcel-to-block crosswalk table.
        block_rent_alternatives: The renter block alternatives table (chunk 3.7).
        price_settings: The ``price_settings`` injectable configuring equilibration.

    Returns:
        None. Mutates the ``households`` table's ``block_geoid`` and ``unit_id``
        columns in place.

    See Also:
        _block_hlcm_simulate: the shared core these four steps delegate to.
        baus.ual.hlcm_renter_lowincome_simulate: the parcel-native step this mirrors.
    """
    _block_hlcm_simulate(households, residential_units, buildings, parcels_block,
                         block_rent_alternatives, price_settings,
                         "hlcm_renter_lowincome.yaml", "rent", "rent_equilibration",
                         True)
