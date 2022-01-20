---
layout: page
title: Models
---

*Work in Progress*

# Models

---
CONTENTS
 
1. [Hazards](#hazards)
2. [Accessibility](#accessibility)
3. [Non-Residential Prices](#nonresidential-prices)
4. [New and Moving Households and Jobs](#new-and-moving-households-and-jobs)
5. [Prices](#prices)
6. [Develop Buildings](#develop-buildings)
7. [Place Households](#place-households)
8. [Place Jobs](place-jobs)
9. [Summaries Validation and Reports](#summaries-validation-and-Reports)

---

## Hazards
### [slr_inundate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/slr.py#L16)
### [slr_remove_dev](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/slr.py#L86)
### [eq_code_buildings](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/earthquake.py#L19)
### [earthquake_demolish](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/earthquake.py#L252)

## Accessibility
### [neighborhood_vars](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L1098)
### [regional_vars](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L1109)

## Nonresidential Prices
### [nrh_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L708)

## New and Moving Households and Jobs
### [household_relocation](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L784)
clarify two versions of this one with s and one wo
### [households_transition](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L33)
### reconcile_unplaced_households 
update building/unit/hh correspondence
### [jobs_relocation](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L307)
### jobs_transition
### balance_rental_and_ownership_hedonics

## Prices 
### [price_vars](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L1148)

## Develop Buildings
### [scheduled_development_events](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L373)
brings in a list
### preserve_affordable
### lump_sum_accounts
### subsidized_residential_developer_lump_sum_accts
run the subsidized residential acct system
### office_lump_sum_accounts
run the subsidized office acct system
### subsidized_office_developer_lump_sum_accts
### [alt_feasibility](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L555)
### [residential_developer](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L581)
### [developer_reprocess](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L893)
### [retail_developer](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L711)
### [office_developer](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L789)
### [accessory_units](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L169)
### calculate_vmt_fees
### [remove_old_units](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L408)
(for buildings that were removed)      
### [initialize_new_units](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L461)
set up units for new residential buildings
### [reconcile_unplaced_households](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L336)
update building/unit/hh correspondence
### [rsh_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L745)
simulates the residential sales hedonic for units
### [rrh_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L764)
simulates the residential rental hedonic for units
### [assign_tenure_to_new_units](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L577)
based on higher of predicted price or rent

## Place Households
### [hlcm_owner_lowincome_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L890)
household location choice model that allocates low income households to owner-occupied units
### [hlcm_renter_lowincome_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L919)
household location choice model that allocates low income households to renter-occupied units
### [hlcm_owner_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L871)
household location choice model that allocates remaining households to owner-occupied units
### [hlcm_renter_simulate](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L905)
household location choice model that allocates remaining households to renter-occupied units
### [hlcm_owner_simulate_no_unplaced](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L1009)
### [hlcm_owner_lowincome_simulate_no_unplaced](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L1029)
### [hlcm_renter_simulate_no_unplaced](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L1049)
### [hlcm_renter_lowincome_simulate_no_unplaced](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L1069)
### [reconcile_placed_households](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L262)

## Place Jobs
### [proportional_elcm](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L185)
start with a proportional jobs model
### [elcm_simulate][https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/models.py#L24)
displaced by new dev

## Summaries Validation and Reports
### OFF [save_intermediate_tables](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/ual.py#L687)
saves output for visualization
### [topsheet](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L458)
write out table related to x
### [simulation_validation](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/validation.py#L110)
### [parcel_summary](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L1327)
write out tables related to x
### [building_summary](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L1303)
write out tables related to x
### [diagnostic_output](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L946)
write out tables related to x
### [geographic_summary](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L1028)
write out tables related to x
### [travel_model_output](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L1467) 
write out tables for inputs to Travel Model 1.5
### OFF [travel_model_2_output](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L1765)
write out tables for inputs to Travel Model 2
### [hazards_slr_summary](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L2266)
write out tables related to sea level rise impacts
### [hazards_eq_summary](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L2350)
writes out tables related to earthquake impacts
### [slack_report](https://github.com/BayAreaMetro/bayarea_urbansim/blob/820554cbabee51725c445b9fd211542db8876c9f/baus/summaries.py#L2493)
sends out information on model completion to Slack
