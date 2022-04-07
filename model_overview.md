---
layout: page
title: Model Overview
---

*Work in Progress*

# Model Overview

---
CONTENTS
 
1. [Introduction](#introduction)
2. [Approach](#approach)
3. [Application Types](#application)
4. [Model System](#model-system)
5. [More Information](#more-information)
6. [Model Schematic](#model-schematic)

---

## Introduction
Bay Area UrbanSim (BAUS) is a land use or urban economic model used to forecast metropolitan growth, study urban policies, and evaluate transportation projects at the Metropolitan Transportation Commission (MTC). It is written in Python and is a customized version of the popular UrbanSim model developed by Professor Paul Waddell over the last few decades. BAUS simulates the movement of households and employees within the region, and the construction of new buildings to hold those households and employees. In this manner, it is used to incrementally forecast potential future urban growth trajectories.

## Approach
A forecast with BAUS begins with a basemap. This is a detailed geodatabase containing all of the region's buildings, households, employees, policies, and transport network for a recent year. This roughly corresponds to tooday's conditions but is a few years in the past due to data collection lag and formalities related to the modeling framework. The buildings are largely an accurate collection of every structure gathered from assessor's data, commercial read estate databases, and other sources. The households and employees are represented at the micro level but their characteristics are built through synthesis (i.e., we only have samples of this informations so we build a full representation that is consistent with these samples). Policies such as zoning and growth limits are collected for each jurisdiction and are binding unless they are explicitly changed for a forecast.

BAUS is used to forecast the future by advancing through repeated steps that chart out a potential pathway for future urban growth. In BAUS each step represents a five year period. Most steps repeat the same set of sub-steps. In UrbanSim, each of these sub-steps is called a "model".  The file used to run BAUS (baus.py) sets the number steps and the order in which the models are are run. The major types of model steps are: 
* (Hazards): This optional model simulates the impact of earthquakes and sea level rise by destroying buildings and displacing their inhabitants.
* Calculate accessibility: Logsum accessibility measures are brought in from the Travel Model and pedestrian accessibility is calculated within UrbanSim
* Calculate housing prices and rents: Hedonic regression models are applied to current (to the model time step) conditions to reestimate current prices and rents
* HH/employee relcation and transition: Some households and employeees are selected to move; additiional households and employees are added or subtracted to reflect the exogenous control totals
* Add buildings from the Development Projects list: Buildings are forced into the model (as opposed to being explicitly modeled); these represent institutional plans (e.g., universities and hospitals), large approved projects (e.g., Treasure Island), and development that has occurred between the basemap year and the current year (i.e., constructed 2016-2020).
* Build new market-rate housing units and commercial space: The for-profit real estate development process is simulated and new buildings are built where they are most feasible; this also build deed-restricted unit that are produced through the market-rate process (e.g., inclusionary zoning).
* Build new deed-restricted affordable units: A similar not-for-profit real estate development process produces affordable housing units based on money available within BAUS Accounts (e.g., bond measures).
* HH/employee location choices: Households and employees are assigned to new locations based on logistic regresssion models that capture the preferences of particular segments (e.g., lower income households, retail jobs).
* Produce summary tables: Numerous zonal summaries of the detailed geodatabaase are produced for analysis of urban change and for use in the Travel Model; this step includes additional post-processing to get the data ready for the Travel Model's population synthesizer.

Typically, a set of BAUS inputs or assumptions are modified to produce a scenario. Policies such as zoning or fees can be modified. Control totals can be adjusted. Assumptions about preferences or financical situations can be adjusted. The package of changes is then simulated to forecast its impact on the future urban landscape and these outcomes are often enterered into the travel model to predict future year travel patterns and greenhouse gas emmissions.

model step are at

### Models

* slr_inundate floods parcels impacted by sea level rise
* slr_remove_dev removes buildings on parcels impacted by sea level rise
* eq_code_buildings 
* earthquake_demolish removes buildings
* neighborhood_vars    # street network accessibility
* regional_vars        # road network accessibility
* nrh_simulate         # non-residential rent hedonic
* household_relocation
* households_transition
* reconcile_unplaced_households update building/unit/hh correspondence
* [jobs_relocation](./models.md#jobs-relocation)
* jobs_transition
* balance_rental_and_ownership_hedonics
* price_vars
* scheduled_development_events brings in a list
* preserve_affordable
* lump_sum_accounts
* subsidized_residential_developer_lump_sum_accts # run the subsidized residential acct system
* office_lump_sum_accounts # run the subsidized office acct system
* subsidized_office_developer_lump_sum_accts
* alt_feasibility
* residential_developer
* developer_reprocess
* [retail_developer](http://bayareametro.github.io/bayarea_urbansim/models/#retail-develper)
* office_developer
* accessory_units
* calculate_vmt_fees
* remove_old_units # (for buildings that were removed)      
* initialize_new_units # set up units for new residential buildings
* reconcile_unplaced_households # update building/unit/hh correspondence
* rsh_simulate simulates the residential sales hedonic for units
* rrh_simulate simulates the residential rental hedonic for units
* assign_tenure_to_new_units based on higher of predicted price or rent
* hlcm_owner_lowincome_simulate household location choice model that allocates low income households to owner-occupied units
* hlcm_renter_lowincome_simulate household location choice model that allocates low income households to renter-occupied units
* hlcm_owner_simulate household location choice model that allocates remaining households to owner-occupied units
* hlcm_renter_simulate household location choice model that allocates remaining households to renter-occupied units
* hlcm_owner_simulate_no_unplaced
* hlcm_owner_lowincome_simulate_no_unplaced
* hlcm_renter_simulate_no_unplaced
* hlcm_renter_lowincome_simulate_no_unplaced
* reconcile_placed_households
* proportional_elcm        # start with a proportional jobs model
* elcm_simulate  # displaced by new dev
* OFF save_intermediate_tables # saves output for visualization
* topsheet write out table related to x
* simulation_validation
* parcel_summary write out tables related to x
* building_summary write out tables related to x
* diagnostic_output write out tables related to x
* geographic_summary write out tables related to x
* travel_model_output write out tables for inputs to Travel Model 1.5
* OFF travel_model_2_output write out tables for inputs to Travel Model 2
* hazards_slr_summary write out tables related to sea level rise impacts
* hazards_eq_summary writes out tables related to earthquake impacts
* slack_report sends out information on model completion to Slack




## Application Types
BAUS is used for for three typical types of application:
* Forecasting: UrbanSim produces a complete map for each five year interval into the future. This information is required by statute and useful for planning processes.
* Policy Studies: BAUS can evaluate the impact of policy changes on, for example, the amount of housing produced, the amount of deed-restricted housing producded, and the locations of future residential or commercial development.
* Transport Project Evaluation: BAUS provides information on future land use patterns used to evaluate the benefits and costs of large transport infrastructure investments and allows an assessment of how such projects may influence future development.


## Model System
BAUS is the middle model in an interactive suite of three model systems maintained by MTC:
* Regional economic and demographic information is supplied to BAUS from the REMI CGE model and related demographic processing scripts. BAUS outputs on housing production are used to adjust regional housing prices (and thus other variables) in REMI. 
* Accessibillity information is supplied to BAUS from the Travel Model and influences real estate prices and household and employee location choices. BAUS outputs on the location of various types of households and employees are used to establish origins and destinations in the Travel Model.


## More Information
Additional information on UrbanSim can be found at urbansim.com.

More information on urban modeling in general can be found at

## Model Schematic
