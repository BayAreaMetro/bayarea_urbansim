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

This is chunk 3.4: the step only assigns each unplaced job a ``block_geoid``; it does
**not** yet set ``building_id`` (that rendering bridge is chunk 3.5). The step is
registered but not yet in the annual model list, and the parcel ``elcm_simulate``
(``baus/models.py``) is left untouched for A/B comparison, so registering this module
changes no model behavior.

See Also:
    baus.block_supply.block_elcm_alternatives: the block alternatives table this step
        chooses among.
    baus.models.elcm_simulate: the parcel-native ELCM this step mirrors and runs
        beside for A/B comparison.
"""

import os

import orca
from urbansim_defaults import utils

from baus.block_developer import _dominant_block_for_parcels

__all__ = [
    # Pure helper (unit-testable, no orca)
    'assign_job_block_geoid',
    # Live orca step (block ELCM choice)
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


@orca.step()
def block_elcm_simulate(jobs, buildings, parcels_block, block_elcm_alternatives):
    """Places unplaced jobs into census blocks with the reused ELCM spec.

    Writes an int64 ``block_geoid`` column onto ``jobs`` (``-1`` for unplaced jobs,
    the dominant block otherwise), then runs ``utils.lcm_simulate`` with the fitted
    ELCM spec against the block alternatives table so each mover chooses a block. The
    chosen ``block_geoid`` is written back onto ``jobs``; ``building_id`` is left
    unchanged (the block-to-building rendering bridge is chunk 3.5).

    Args:
        jobs: The orca ``jobs`` table (choosers); unplaced jobs have
            ``building_id == -1``.
        buildings: The orca ``buildings`` table, used to map placed jobs to parcels.
        parcels_block: The areal parcel-to-block crosswalk table, used to derive each
            parcel's dominant block.
        block_elcm_alternatives: The block-indexed ELCM alternatives table (chunk
            3.3) with ``job_spaces`` supply and integer ``vacant_job_spaces`` vacancy.

    Returns:
        None. Mutates the ``jobs`` table's ``block_geoid`` column in place.

    See Also:
        assign_job_block_geoid: the pure helper that builds the initial int64 key.
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
