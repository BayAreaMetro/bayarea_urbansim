

# Plan Bay Area 2050+ BAUS Strategy Inputs

This page documents strategy input files used in BAUS modeling for *Plan Bay Area 2050+*. The primary purpose of this documentation is to clearly identify and record the specific versions of input files used in model runs for both the *No Project Alternative* and the *Plan* scenarios.  It also includes a high-level overview of how inputs are utilized in BAUS.

---

## Table of Contents
1. [PBA 2050+ No Project Alternative](#pba2050-no-project-alternative)
   
   - [Regional Controls](#regional-controls)
   - [Base Zoning](#base-zoning)
   - [Inclusionary Zoning](#inclusionary-zoning)
   - [Development Pipeline](#development-pipeline)
   - [Exogenous Worker Assumptions](#exogenous-assumptions)
   - [Travel Model Logsums](#travel-model-logsums)
   - [Sea Level Rise](#sea-level-rise)
   
   
2. [PBA 2050+ Plan](#pba2050-plan-scenario)
   - [EC1: Guranteed Income](#ec1-guranteed-income)
   - [EC2: Job Training](#ec2-expand-job-training-and-incubator-programs)
   - [EC4: Zoning Mods](#ec4-allow-a-greater-mix-of-land-uses-and-densities-in-growth-geographies)
   - [EC5: Employer Relocation](#ec5-provide-incentives-to-employers-to-locate-in-low-vmt-areas)
   - [EC6: Industial Land Asserted Projects](#strategy-ec6-retain-and-invest-in-key-industrial-lands)
   - [H1: Protection](#h1-further-strengthen-renter-protections-beyond-state-law)
   - [H2: Preservation](#h2-preserve-existing-affordable-housing)
   - [H3: Housing Mix](#h3-allow-a-greater-mix-of-housing-densities-and-types-in-growth-geographies)
   - [H4: Produce Affordable Housing](#h4-build-adequate-affordable-housing-to-ensure-homes-for-all)
   - [H5: Inclusionary](#h5-integrate-affordable-housing-into-all-major-housing-projects)
   - [H6: Transform Malls/Office Parks Asserted Projects](#h6-transform-aging-malls-and-office-parks-into-neighborhoods)
   - [H8: Public Land Asserted Projects](#h8-accelerate-reuse-of-public-and-community-owned-land-for-mixed-income-housing-and-essential-services)
   - [EN1: Adapt to Sea Level Rise](#en1-adapt-to-sea-level-ris)
   - [Travel Model Logsums](#travel-model-logsums-1)
3. [Other Activated Policy](#other-activated-policy)
    - [SB 743](#sb-743)
    - [ADUs](#adus)

---


## PBA2050+ No Project Alternative
The No Project alternative represents the expected trajectory of the region without the
implementation of the Plan. All policies in the No Project alternative are determined or
extrapolated from existing base year plans and policies.  Staff produced and evaluated many No Project candidates.  The BAUS run for the "Final" No Project alternative can be found in following directory:
  - `M:\urban_modeling\baus\PBA50Plus\PBA50Plus_NoProject\PBA50Plus_NoProject_v38`  



### Regional Controls
Regional control totals of households and jobs were produced using REMI 3.1 and additional demographic processing scripts.  BAUS uses these projections to incrementally forecast urban growth trajectories.

**Input**
- `household_controls_file`: 
  - `M:\urban_modeling\baus\BAUS Inputs\regional_controls\household_controls_PBA50Plus_NP.csv`
  - Loaded via [`datasources.household_controls_unstacked`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1159C5-L1159C33)
- `employment_controls_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\regional_controls\employment_controls_PBA50Plus_NP_DBP.csv`
  - Loaded via [`datasources.employment_controls_unstacked`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1195C5-L1195C34)



### Base Zoning
The parcel zoning and zoning lookup file were sourced from BASIS.  Base zoning was derived from local zoning ordinances or General Plans.  `zoning_file` includes a zoning id at the parcel-level.  `zoning_lookup_file` relates the zoning id to attributes such as `max_dua`, `max_far`, and `max_height`.  Development types are represented by 14 two-letter dummy variables, as shown in this [lookup file](https://github.com/BayAreaMetro/petrale/blob/master/incoming/dv_buildings_det_type_lu.csv).  The developer model works within these existing zoning constraints in the No Project alternative.

**Input**
- `zoning_file`:  
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\zoning\zoning_parcels_2024-10-14.csv`  
  - Loaded via [`datasources.zoning_existing`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L505)
- `zoning_lookup_file`:  
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\zoning\zoning_lookup_2024-10-14.csv`  
  - Loaded via [`datasources.zoning_lookup`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L492)


### Inclusionary Zoning  
Existing inclusionary zoning requirements sourced from local zoning ordinances or housing policies. The developer model must "set-aside" the required share of affordable units when producing new residential projects.

**Input**  
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\existing_policy\inclusionary.yaml`
  - Loaded via [`datasources.inclusionary`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L182)

### Development Pipeline
BASIS-sourced development pipeline.  Implemented in the model's scheduled development events.

**Input**
- `development_pipeline_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\parcels_buildings_agents\development_pipeline_NP_2024-03-08.csv`
  - Loaded via [`datasources.get_dev_projects_table`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L991C5-L991C27)


### Exogenous Assumptions
Space per worker assumptions.  Includes multipliers which adjust the sqft per job setting by superdistrict by year to represent changes due to increased telecommuting post-2020.  Determines the number of jobs that could occupy a building.  This affects the total jobs produced in the model's scheduled development events and the retail developer model.

**Input**
- `exog_sqft_per_job_adj_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\sqft_per_job_adjusters_costar_qcew_timevarying_base_2023_0p85_reduction_dec2024_sd_2035.csv`
  - Loaded via [`datasources.exog_sqft_per_job_adjusters`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1291C5-L1291C32)


### Travel Model Logsums 
No Project logsums representing transportation utility/attractiveness of different locations.   Allows future year travel conditions to influence real estate prices in BAUS.


**Input**
- `logsum_file1`: 
  - `M:\urban_modeling\baus\BAUS Inputs\accessibility\travel_model\subzone_logsums_for_BAUS_PBA50Plus_NP_13_2035.csv`
- `logsum_file2`: 
  - `M:\urban_modeling\baus\BAUS Inputs\accessibility\travel_model\subzone_logsums_for_BAUS_PBA50Plus_NP_14_2050.csv`
  - Both loaded via [`datasources.taz_logsums`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L947C5-L947C16)


### Sea Level Rise
The SLR inundation file includes details on sea level rise at which each inundation parcel becomes inundated, for each forecast year. In the No Project alternative, rows marked with "100" are parcels with committed projects where sea level rise has been mitigated.  Inundated parcels are removed from the universe of parcels that BAUS can act upon.


**Input**
- `slr_inundation_file`: 
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\hazards\urbansim_slr_no_project_MAR2025.csv`
  - Loaded via [`datasources.slr_parcel_inundation`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1361C5-L1361C26)
- `slr_progression_file`: 
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\hazards\slr_progression_PBA50Plus.csv` 
  - Loaded via [`datasources.slr_progression`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1347C5-L1347C20)


---

## PBA2050+ Plan Scenario

The Plan scenario includes the plan's strategies and investments.  The BAUS run for the "Final" Plan scenatio can be found in following directory:
- `M:\urban_modeling\baus\PBA50Plus\PBA50Plus_FinalBlueprint\PBA50Plus_Final_Blueprint_v65`

### EC1: Guranteed Income
Regional control totals of households and jobs were produced using REMI 3.1 and additional demographic processing scripts.  In the Plan scenario, strategy EC1 is captured by updating the income distribution results outside the REMI model to represent the strategy's impact,  effectively shifting a portion of Q1 households to Q2. 


**Input**
- `household_controls_file`: 
  - `M:\urban_modeling\baus\BAUS Inputs\regional_controls\household_controls_PBA50Plus_DBP_UBI2030.csv`
  - Loaded via [`datasources.household_controls_unstacked`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1159C5-L1159C33)
- `employment_controls_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\regional_controls\employment_controls_PBA50Plus_NP_DBP.csv`
  - Loaded via [`datasources.employment_controls_unstacked`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1195C5-L1195C34)


### EC2: Expand Job Training and Incubator Programs

EC2 funds the development of 25 new incubator spaces in transit-rich PPAs, prioritizing those within EPCs. These spaces are modeled in BAUS as 250,000–500,000 square feet of commercial or industrial development per site through the Scheduled Development Events sub-model, spread equally over the forecast period. Commercial incubator spaces are represented as as retail buildings. Over time, the Employment Location Choice model seeks to place jobs in these incubator buildings.

**Input**
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388)
- `dev_pipeline_strategies`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\EC2_development_pipeline_entries.csv`
  - Loaded via [`datasources.get_dev_projects_table`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L991C5-L991C27)



### EC4: Allow a Greater Mix of Land Uses and Densities in Growth Geographies 
EC4 modifies zoning schemas applied at the parcel level, allowing new building types and/or increased commercial development densitities not already permitted by local zoning. These modifications are guided by the Growth Geographies framework.  The developer model operates within the boundaries of the zoning modifications and guided by the growth geography framework.  Increased commercial zoning often coincides with denser residential zoning, creating mixed-use environments where these uses compete and complement each other. 


**Input**
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388)
- `zoning_mods_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\zoning_mods_PBA50Plus_FBP_pgfeb14_2025_ppafix.csv`
  - Loaded via [`datasources.zoning_strategy`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L726C5-L726C20)
- `zoningmodcat_cols`:
  - `["gg_id", "exd_id", "tra_id", "hra_id", "ppa_id", "ugb_id"]`


### EC5: Provide Incentives to Employers to Locate in Low-VMT Areas
EC5 is modeled in BAUS by first creating an additional 25 million square feet of non-residential real estate in transit friendly locations, with buildings of 250,000 square feet added throughout the region through the Scheduled Development Events sub-model, spread equally over the forecast period. Over time, the Employment Location Choice model simulates incentives to locate jobs in these buildings.

**Input**
- `dev_pipeline_strategies`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\EC5_development_pipeline_anchor_buildings_FBP_v4.csv`
  - Loaded via [`datasources.get_dev_projects_table`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L991C5-L991C27)
- `ec5 parcels file`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\parcels_p10_x_ec5_v2.csv`
  - Loaded via [`datasources.ec5_parcels`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L1494C5-L1494C16)  



### Strategy EC6: Retain and Invest in Key Industrial Lands
EC6 is reflected in BAUS by maintaining industrial zoning in PPAs, allowing industrial use without competition from multifamily use. Development capacity in these PPAs is increased to a max FAR of 0.35 (or local zoning where higher) to accommodate new industrial development. Since BAUS doesn’t explicitly model industrial buildings, the main function of the industrial zoning is to preclude sites turning into office or residential centers.  

To further stimulate economic development in these areas, strategy projects were added to PPA parcels as scheduled development events, spread equally over the forecast period. The scheduled development events sub-model then constructed these projects in their respective future years. 

**Input**
- `dev_pipeline_strategies:`
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\EC6_development_pipeline_entries.csv`
  - Loaded via [`datasources.get_dev_projects_table`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L991C5-L991C27)


### H1: Further Strengthen Renter Protections Beyond State Law  
H1 is reflected in the household relocation model as a 15% decrease in the relocation rate for low-income households, resulting in a 67% relocation probability within each five-year model time step. Consequently, low-income renter households remain in their homes longer than other household groups as the region grows and the land use pattern evolves.  

**Input**  
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388)  
- `renter relocation rates file`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\renter_protections_relocation_rates_overwrites.csv`
  - Loaded via [`datasources.renter_protections_relocation_rates`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1437).


### H2: Preserve Existing Affordable Housing 
H2 allocates preservation funding for housing in TRAs, DRAs, or general GGs (non PPAs).  Funding is
specified by county, based on the base year number of low-income households in these geographies
without deed-restricted affordable housing, who are susceptible to increasingly unaffordable rents
without the preservation strategy.  BAUS applies affordable housing funds by randomly selecting housing units for preservation. Once preserved, these subsidized units are prioritized for low-income households in the model.

**Input**  
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388)  
- `preservation_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\preservation_PBA50Plus_FBP_APR26_2025.yaml`
  - Loaded via [`datasources.preservation`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L205)


### H3: Allow a Greater Mix of Housing Densities and Types in Growth Geographies
H3 modifies zoning schema at parcel level to broaden allowable building types and increase development density in TRAs and HRAs. Zoning also differs between parcels containing residential units and parcels not containing residential units to better account for local context and existing conditions.  The developer model then works within the modified zoning capacity, guided by the growth geography framework.

**Input** 
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388) 
- `parcels_geography_file`
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\crosswalks\fbp_urbansim_parcel_classes_ot50pct_feb25_rwc_update_2025.csv`
  - Loaded via [`datasources.parcels_geography`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L864)
- `zoning_mods_file`:  
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\zoning_mods_PBA50Plus_FBP_pgfeb14_2025_ppafix.csv`
  - Loaded via [`datasources.zoning_strategy`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L726)
- `zoningmodcat_cols`:
  - `["gg_id", "exd_id", "tra_id", "hra_id", "ppa_id", "ugb_id"]`



### H4: Build Adequate Affordable Housing to Ensure Homes for All

H4 allocates additonal funding to produce new deed-restricted housing within the Growth Geographies during the forecast period.  BAUS first identifies residential development projects that are close to being financially feasible under market conditions, then subsidizes these projects to fill the "feasibility gap"; the financial need of these projects is sorted to maximize the number of projects that can become feasible with the given funding. These projects create deed-restricted units exclusively for low-income households.

**Input**  
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388) 
- `account_strategies_file`:  
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\account_strategies_PBA50Plus_FBP.yaml`  
  - [`datasources.account_strategies`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L124C5-L124C23)


### H5: Integrate Affordable Housing into All Major Housing Projects

H5 adjusts the base year inclusionary percentages to meet growth geography-specific requirements. BAUS reflects the requirement by modifying the financial feasibility of new residential development projects. If a project remains profitable, the affordable units will be constructed. Similar to other affordable units, inclusionary units prioritize households in the lowest income quantile. 

**Input**
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388) 
- `inclusionary_strategy_file`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\inclusionary_strategy_PBA50Plus_FBP_pgfeb25_25.yaml`
  - Loaded via [`datasources.inclusionary_strategy`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L193) and [`datasources.inclusionary_housing_settings`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L336C5-L336C34)


### H6: Transform Aging Malls and Office Parks into Neighborhoods
H6 permits and promotes the reuse of shopping malls and office parks with limited commercial viability as neighborhoods with housing for residents at all income levels, including locally-designated Priority Sites. These projects are modeled through the Scheduled Development Events sub-model, where representative new projects are generated on these sites.  

**Input**
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388) 
- `dev_pipeline_strategies`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\H6_H8_development_pipeline_entries_FBP_MAR.csv`
  - Loaded via [`datasources.get_dev_projects_table`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L972)



### H8: Accelerate Reuse of Public and Community-Owned Land for Mixed-Income Housing and Essential Services
H8 aims to accelerate the reuse of surplus public land and land owned by non-profit institutions for developing housing, including affordable housing, and local services. Similar to H6, these projects are developed through the Scheduled Development Events sub-model. These developments were primarily 100% affordable housing projects, with some mixed-used projects to add commercial space for services.  


**Input**
- [**PBA50Plus_FinalBlueprint_ModelingParameters_Housing+Economy Strategies_v3.xlsx**
](https://mtcdrive.app.box.com/file/1855394781388) 
- `dev_pipeline_strategies`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\H6_H8_development_pipeline_entries_FBP_MAR.csv`
  - Loaded via [`datasources.get_dev_projects_table`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L972)


### EN1: Adapt to Sea Level Ris
To reflect EN1, protected areas are spared from inundation by altering input files that specify
inundated parcels. When a parcel is removed from the inundation set, households and jobs are no
longer displaced, allowing the land to be available for new development to accommodate regional
growth.


**Input**
- `slr_inundation_file`: 
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\hazards\urbansim_slr_MAR2025.csv`
  - Loaded via [`datasources.slr_parcel_inundation`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1361C5-L1361C26)
- `slr_progression_file`: 
  - `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\hazards\slr_progression_PBA50Plus.csv` 
  - Loaded via [`datasources.slr_progression`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L1347C5-L1347C20)

### Travel Model Logsums 
Plan logsums representing transportation utility/attractiveness of different locations.   Allows future year travel conditions to influence real estate prices in BAUS.

**Input**
- `logsum_file1`: 
  - `M:\urban_modeling\baus\BAUS Inputs\accessibility\travel_model\subzone_logsums_for_BAUS_PBA50Plus_FBP_13_2035.csv`
- `logsum_file2`: 
  - `M:\urban_modeling\baus\BAUS Inputs\accessibility\travel_model\subzone_logsums_for_BAUS_PBA50Plus_FBP_14_2050.csv`
  - Both loaded via [`datasources.taz_logsums`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/2813a26c2394a8c1bc353bd9f70e5df83006f379/baus/datasources.py#L947C5-L947C16)

---


## Other Activated Policy

### SB-743
743 shifts analysis to a VMT method that is more likely to find transportation impacts in car-oriented suburban locations. This permanently activated strategy adjusts profits based on TAZ-level VMT categories.

**Input**  
- `vmt fee file`:
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\vmt_fee_zonecats.csv`
  - Loaded via [`datasources.vmt_fee_categories`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L1238)



### ADUs
Adds accessory dwelling units (ADUs) based on targets.

**Input**
- `ADU file`:  
  - `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\accessory_units.csv`
  - Loaded via [`datasources.accessory_units`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/datasources.py#L1429)  
[`models.accessory_units_strategy`](https://github.com/BayAreaMetro/bayarea_urbansim/blob/e9259329837c31a2283830d0d83ecebdeaab9a59/baus/models.py#L274)  