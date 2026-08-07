"""Block-level employment location choice (ELCM) for the BAUS block port.

This module wires the existing ELCM spec (``configs/location_choice/elcm.yaml``,
reused unchanged) onto the census-block alternatives table built in Phase-3 chunk
3.3 (``baus.block_supply.block_elcm_alternatives``). Instead of choosing among
*buildings*, unplaced jobs choose among *blocks*: ``utils.lcm_simulate`` runs with
``out_fname='block_geoid'``, ``supply_fname='job_spaces'`` and
``vacant_fname='vacant_job_spaces'`` pointed at the block table, so the fitted ELCM
coefficients score block covariates directly (Option B — covariates materialized on
the block table, no ``join_tbls`` broadcast).

``utils.lcm_simulate`` identifies movers as ``choosers[out_fname] == -1`` and expands
vacancy by repeating the alternatives' index, so the block key must be a numeric
column with a ``-1`` unplaced sentinel. ``parcels_block`` therefore loads
``block_geoid`` as int64 (the 15-digit GEOID fits int64), and this step writes an
int64 ``block_geoid`` column onto ``jobs``: ``-1`` for unplaced jobs (``building_id
== -1``), and the job's building's parcel's dominant block for placed jobs.

The step then renders each block choice down to a concrete building (chunk 3.5): a
thin, behavior-free bridge picks a building with ``vacant_job_spaces > 0`` in the
chosen block and writes ``building_id`` back onto ``jobs``, so ``elcm_simulate``'s
supply accounting and every downstream job-geography variable/summary keep working
unchanged. Assignment within a block is a deterministic capacity-fill (stable order,
no RNG). The step is registered but not yet in the annual model list, and the parcel
``elcm_simulate`` (``baus/models.py``) is left untouched for A/B comparison, so
registering this module changes no model behavior.

See Also:
    baus.block_supply.block_elcm_alternatives: the block alternatives table this step
        chooses among.
    baus.models.elcm_simulate: the parcel-native ELCM this step mirrors and runs
        beside for A/B comparison.
"""

import os

import orca
import pandas as pd
from urbansim_defaults import utils

from baus.block_developer import _dominant_block_for_parcels

__all__ = [
    # Pure helpers (unit-testable, no orca)
    'assign_job_block_geoid',
    'render_block_jobs_to_buildings',
    # Live orca step (block ELCM choice + rendering bridge)
    'block_elcm_simulate',
]


def assign_job_block_geoid(jobs, buildings, dominant_block):
    """Assigns each job the int64 block key ``utils.lcm_simulate`` chooses from.

    Unplaced jobs (``building_id == -1``) receive the ``-1`` sentinel that
    ``lcm_simulate`` uses to identify movers; placed jobs receive the dominant
    (largest-area-share) block of their building's parcel. The result is an int64
    column so it can serve as the numeric alternatives key.

    Args:
        jobs: DataFrame with a ``building_id`` column (one row per job); an unplaced
            job is flagged by ``building_id == -1``.
        buildings: DataFrame indexed by ``building_id`` with a ``parcel_id`` column.
        dominant_block: Series indexed by ``parcel_id`` giving each parcel's dominant
            int64 ``block_geoid`` (see ``_dominant_block_for_parcels``).

    Returns:
        An int64 Series aligned to ``jobs`` with ``-1`` for unplaced jobs and the
        placed job's parcel's dominant block otherwise.

    Example:
        >>> import pandas as pd
        >>> jobs = pd.DataFrame({"building_id": [100, -1]})
        >>> buildings = pd.DataFrame(
        ...     {"parcel_id": [7]}, index=pd.Index([100], name="building_id"))
        >>> dominant_block = pd.Series(
        ...     [60750611012023], index=pd.Index([7], name="parcel_id"))
        >>> assign_job_block_geoid(jobs, buildings, dominant_block).tolist()
        [60750611012023, -1]

    See Also:
        block_elcm_simulate: the orca step that writes this column onto ``jobs`` and
            runs the block ELCM.
    """
    job_parcel = jobs["building_id"].map(buildings["parcel_id"])
    job_block = job_parcel.map(dominant_block)
    is_placed = jobs["building_id"] != -1
    return job_block.where(is_placed, other=-1).astype("int64")


def render_block_jobs_to_buildings(jobs, buildings, dominant_block):
    """Renders each block-placed job down to a concrete building in that block.

    The block ELCM leaves a mover with a chosen ``block_geoid`` but still
    ``building_id == -1``. This thin, behavior-free bridge assigns each such job a
    building with ``vacant_job_spaces > 0`` in its chosen block, filling candidate
    buildings deterministically (stable ``building_id`` order, no RNG) so a building
    is never assigned more jobs than its vacant job spaces. Jobs whose chosen block
    lacks enough vacant building capacity keep ``building_id == -1`` (the normal
    unplaced tail), so the returned Series covers only the jobs actually rendered.

    A building's block is its parcel's dominant block, so the candidate buildings in a
    block are exactly those the block ELCM's vacancy was rolled up from (up to the
    areal-vs-dominant seam noted in the module design).

    Args:
        jobs: DataFrame with ``building_id`` and int64 ``block_geoid`` columns; a job
            to render has ``building_id == -1`` and ``block_geoid != -1``.
        buildings: DataFrame indexed by ``building_id`` with ``parcel_id`` and integer
            ``vacant_job_spaces`` columns.
        dominant_block: Series indexed by ``parcel_id`` giving each parcel's dominant
            int64 ``block_geoid`` (see ``_dominant_block_for_parcels``).

    Returns:
        An int64 Series indexed by job id, holding the assigned ``building_id`` for
        each rendered job only (jobs left unplaced are omitted).

    Example:
        >>> import pandas as pd
        >>> buildings = pd.DataFrame(
        ...     {"parcel_id": [7], "vacant_job_spaces": [2]},
        ...     index=pd.Index([100], name="building_id"))
        >>> dominant_block = pd.Series(
        ...     [55], index=pd.Index([7], name="parcel_id"))
        >>> jobs = pd.DataFrame({"building_id": [-1, -1], "block_geoid": [55, 55]})
        >>> render_block_jobs_to_buildings(jobs, buildings, dominant_block).tolist()
        [100, 100]

    See Also:
        block_elcm_simulate: the orca step that applies these assignments to ``jobs``.
        assign_job_block_geoid: the companion helper that assigns the block key.
    """
    building_block = buildings["parcel_id"].map(dominant_block)
    vacancy = buildings["vacant_job_spaces"].astype("int64")
    slots = pd.DataFrame({
        "block_geoid": building_block.values,
        "building_id": buildings.index.values,
        "vacant_job_spaces": vacancy.values,
    })
    slots = slots[slots["vacant_job_spaces"] > 0].sort_values("building_id")
    slots = slots.loc[slots.index.repeat(slots["vacant_job_spaces"])].reset_index(drop=True)
    slots["_slot_rank"] = slots.groupby("block_geoid").cumcount()

    to_render = jobs[(jobs["building_id"] == -1) & (jobs["block_geoid"] != -1)]
    to_render = to_render.sort_index().reset_index()
    job_id_col = to_render.columns[0]
    to_render["_slot_rank"] = to_render.groupby("block_geoid").cumcount()

    matched = to_render[[job_id_col, "block_geoid", "_slot_rank"]].merge(
        slots[["block_geoid", "building_id", "_slot_rank"]],
        on=["block_geoid", "_slot_rank"], how="inner")
    return pd.Series(
        matched["building_id"].values,
        index=matched[job_id_col].values,
        name="building_id").astype("int64")


@orca.step()
def block_elcm_simulate(jobs, buildings, parcels_block, block_elcm_alternatives):
    """Places unplaced jobs into census blocks and renders them to buildings.

    Writes an int64 ``block_geoid`` column onto ``jobs`` (``-1`` for unplaced jobs,
    the dominant block otherwise), then runs ``utils.lcm_simulate`` with the fitted
    ELCM spec against the block alternatives table so each mover chooses a block. The
    chosen ``block_geoid`` is written back onto ``jobs``, then the rendering bridge
    (``render_block_jobs_to_buildings``) assigns each newly block-placed job a
    building with vacant job spaces in that block and writes ``building_id`` back.

    Args:
        jobs: The orca ``jobs`` table (choosers); unplaced jobs have
            ``building_id == -1``.
        buildings: The orca ``buildings`` table, used to map placed jobs to parcels
            and to source per-building vacant job spaces for rendering.
        parcels_block: The areal parcel-to-block crosswalk table, used to derive each
            parcel's dominant block.
        block_elcm_alternatives: The block-indexed ELCM alternatives table (chunk
            3.3) with ``job_spaces`` supply and integer ``vacant_job_spaces`` vacancy.

    Returns:
        None. Mutates the ``jobs`` table's ``block_geoid`` and ``building_id`` columns
        in place.

    See Also:
        assign_job_block_geoid: the pure helper that builds the initial int64 key.
        render_block_jobs_to_buildings: the pure helper that renders block choices to
            buildings.
        baus.block_supply.block_elcm_alternatives: the alternatives table chosen from.
    """
    parcel_block = parcels_block.to_frame(["block_geoid", "parcel_block_share"])
    dominant_block = _dominant_block_for_parcels(parcel_block)

    jobs_df = jobs.to_frame(["building_id"])
    buildings_df = buildings.to_frame(["parcel_id"])
    job_block_geoid = assign_job_block_geoid(jobs_df, buildings_df, dominant_block)
    jobs.update_col("block_geoid", job_block_geoid)

    spec_path = os.path.join("location_choice", orca.get_injectable("elcm_spec_file"))
    utils.lcm_simulate(spec_path,
                       jobs, block_elcm_alternatives, [],
                       "block_geoid", "job_spaces",
                       "vacant_job_spaces", cast=True)

    render_buildings = buildings.to_frame(["parcel_id", "vacant_job_spaces"])
    jobs_after = jobs.to_frame(["building_id", "block_geoid"])
    building_updates = render_block_jobs_to_buildings(
        jobs_after, render_buildings, dominant_block)
    jobs.update_col_from_series("building_id", building_updates, cast=True)
