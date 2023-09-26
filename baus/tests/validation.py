from __future__ import print_function

import sys
import os
import orca
import pandas as pd
from pandas.util import testing as pdt
from baus.utils import save_and_restore_state
from urbansim.utils import misc


def assert_series_equal(s1, s2, head=None):
    s1 = s1.sort_index()
    s2 = s2.sort_index()
    if head:
        # for really long series, it makes sense to only compare the first
        # head number of items in each series
        s1 = s1.head(head)
        s2 = s2.head(head)
    pdt.assert_series_equal(s1, s2, check_names=False, check_dtype=False)


# make sure the household controls are currently being matched
def check_household_controls(households, household_controls, year):
    print("Check household controls")
    current_household_controls = household_controls.local.loc[year]
    current_household_controls = current_household_controls.\
        set_index("base_income_quartile").total_number_of_households

    assert_series_equal(
        current_household_controls,
        households.hh_inc_cat1.value_counts()
    )


# make sure the employment controls are currently being matched
def check_job_controls(jobs, employment_controls, year, developer_settings):
    print("Check job controls")
    current_employment_controls = employment_controls.local.loc[year]
    current_employment_controls = current_employment_controls.\
        set_index("empsix_id").number_of_jobs

    empsix_map = developer_settings["empsix_name_to_id"]
    current_counts = jobs.emp6_cat.map(empsix_map).value_counts()

    assert_series_equal(
        current_employment_controls,
        current_counts
    )


# make sure everyone gets a house - this might not exist in the real world,
# but due to the nature of control totals it exists here
def check_no_unplaced_households(households, year):
    print("Check no unplaced households")
    assert -1 not in households.building_id.value_counts()


def check_no_unplaced_jobs(jobs, year):
    print("Check no unplaced jobs")
    assert -1 not in jobs.building_id.value_counts()


# check not more households than units or jobs than job spaces
def check_no_overfull_buildings(buildings):
    print("Check no overfull buildings")
    assert True not in (buildings.vacant_res_units < 0).value_counts()
    # there are overfull job spaces based on the assignment and also proportional job model
    # assert True not in (buildings.vacant_job_spaces < 0).value_counts()


@orca.step()
def simulation_validation(buildings, households, jobs, residential_units, year,
                          household_controls, employment_controls, developer_settings):

    check_job_controls(jobs, employment_controls, year, developer_settings)

    check_household_controls(households, household_controls, year)

    check_no_unplaced_households(households, year)

    check_no_unplaced_jobs(jobs, year)

    check_no_overfull_buildings(households, buildings)

    return