from __future__ import print_function
import os
import sys
import yaml
import numpy as np
import pandas as pd
import orca
import pandana.network as pdna
from urbansim.developer import sqftproforma
from urbansim.developer.developer import Developer as dev
from urbansim.utils import misc, networks
from urbansim_defaults import models, utils
from baus import datasources, subsidies, summaries, variables
from baus.utils import add_buildings, groupby_random_choice, parcel_id_to_geom_id, round_series_match_target


@orca.step()
def households_transition(households, household_controls, year, settings):
    s = orca.get_table('households').base_income_quartile.value_counts()
    print("Distribution by income before:\n", (s/s.sum()))
    ret = utils.full_transition(households,
                                household_controls,
                                year,
                                transition_relocation['households_transition'],
                                "building_id")
    s = orca.get_table('households').base_income_quartile.value_counts()
    print("Distribution by income after:\n", (s/s.sum()))
    return ret


@orca.step()
def jobs_relocation(jobs, employment_relocation_rates, years_per_iter,
                    settings, static_parcels, buildings):

    # get buildings that are on those parcels
    static_buildings = buildings.index[
        buildings.parcel_id.isin(static_parcels)]

    df = pd.merge(jobs.to_frame(["zone_id", "empsix"]),
                  employment_relocation_rates.local,
                  on=["zone_id", "empsix"],
                  how="left")

    df.index = jobs.index

    # get the move rate for each job
    rate = (df.rate * years_per_iter).clip(0, 1.0)
    # get random floats and move jobs if they're less than the rate
    move = np.random.random(len(rate)) < rate

    # also don't move jobs that are on static parcels
    move &= ~jobs.building_id.isin(static_buildings)

    # get the index of the moving jobs
    index = jobs.index[move]

    # set jobs that are moving to a building_id of -1 (means unplaced)
    jobs.update_col_from_series("building_id",
                                pd.Series(-1, index=index))

@orca.step()
def household_relocation(households, household_relocation_rates,
                         settings, static_parcels, buildings):

    # get buildings that are on those parcels
    static_buildings = buildings.index[buildings.parcel_id.isin(static_parcels)]

    # UPDATE to map rates by config: rent/own probability
    df = pd.merge(households.to_frame(["zone_id", "base_income_quartile", "tenure"]), household_relocation_rates.local,
                  on=["zone_id", "base_income_quartile", "tenure"], how="left")

    df.index = households.index

    # get random floats and move households if they're less than the rate
    move = np.random.random(len(df.rate)) < df.rate

    # also don't move households that are on static parcels
    move &= ~households.building_id.isin(static_buildings)

    # get the index of the moving jobs
    index = households.index[move]
    print("{} households are relocating".format(len(index)))

    # set households that are moving to a building_id of -1 (means unplaced)
    households.update_col_from_series("building_id",
                                      pd.Series(-1, index=index), cast=True)


# this deviates from the step in urbansim_defaults only in how it deals with
# demolished buildings - this version only demolishes when there is a row to
# demolish in the csv file - this also allows building multiple buildings and
# just adding capacity on an existing parcel, by adding one building at a time