### Exogenous Assumptions (NoProject)
TBD

### Existing Policies (NoProject)

#### Planned Land Use (base zoning, from local zoning ordinance or General Plan)
* Two input files:
	* `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\zoning\zoning_parcels.csv`, maps each parcel to its zoning_id. Read into the model via [def zoning_existing](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/datasources.py#L460C1-L460C20).
	* `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\zoning\zoning_lookup.csv`, specifies development contraints for each zoning_id (column "id"), including: maximum allowable intensity (max_far, max_dua, max_height) and allowable development type. Development type is represented by 14 two-letter dummy variables as shown in this [lookup file](https://github.com/BayAreaMetro/petrale/blob/master/incoming/dv_buildings_det_type_lu.csv)). Read into the model via [def zoning_lookup](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/datasources.py#L447).

#### Inclusionary zoning
`M:\urban_modeling\baus\BAUS Inputs\basis_inputs\existing_policy\inclusionary.yaml`: existing inclusionary zoning requirements, sourced from local zoning ordinance or housing policies.

### Plan Bay Area Strategies (PBA50+)
Modeling notes on PBA50 strategies are at https://github.com/BayAreaMetro/bayarea_urbansim/wiki/BAUSStrategyCodingNotes. 

#### Zoning Modifications (H3, EC4, EC6)
* PBA50+ Strategies: H3 "Allow a Greater Mix of Housing Densities and Types in Growth Geographies", EC4 "Allow a Greater Mix of Land Uses and Densities in Growth
Geographies", EC6 "Retain and Invest in Key Industrial Lands".
* Input file: 
    * zoning_mods_file: `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\zoning_mods_[version].csv`, sets allowable development types/intensities and sets development restrictions based on a combination of geographies. In general, it increases the allowable density (dua_up and far_up) and allows denser development types (e.g. HM - multi-family residential) in areas where the Plan aims to focus future growth in, such as parcels within GG and TRAs, decreases the allowable density or sets it to zero (dua_down and far_down) in places where the Plan aims to avoid sprawling, such as areas outside of the Urban Growth Boundaries; in certain areas such as PPAs (Priority Production Areas), it limits residential development and encourages non-residential development. The allowable development type/intensity is defined based on a combination of geographies, defined by [zoningmodcat_cols](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/run_setup_PBA50Plus_DraftBlueprint.yaml#L130C1-L130C18). An update in the Growth Geographies methodology or data may require updating the zoningmodcat_cols.
    * parcels_geography_file: `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\crosswalks\parcels_geography.csv` or `M:\urban_modeling\baus\BAUS Inputs\basis_inputs\crosswalks\parcel_classes.csv`, tags each parcel based on its geography designations, e.g. the "gg_id" column shows whether a parcel is within the Growth Geographies ("gg") or not (nan), the "tra_id" column shows whether a parcel is within a TRA tier ("tra1", "tra2", etc.). Note that the parcel-geographies lookup is used for all strategies with a geographic aspect, not limited to zoning modification.
* Coding notes:
	* parcels_geography_file is read into the model via [def parcels_geography](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/datasources.py#L819C1-L819C22).
    * zoning_mods_file is read into the model and joined to parcels_geography via [def zoning_strategy](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/datasources.py#L681C1-L681C20), which is then joined to the master parcels table. 
	* In Blueprint scenario, the model assesses base zoning and zoning modification, and calculates the [effective_max_dua](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/variables.py#L1000) and [effective_max_far](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/variables.py#L1047) of each parcel, usually the higher of the allowed densities of base zoning and zoning modification. Then the model applies some other development restrictions (such as "not developable parcels", e.g. churches and parks, defined as "nodev") and adjustments (e.g. adjusting for edge cases) and arrives at [max_dua](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/variables.py#L832) and [max_far](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/variables.py#L801). These two parcel attributes are later used in the [feasibility](https://github.com/BayAreaMetro/bayarea_urbansim/blob/f4215d2480cf87183e5e52685fcc0d7542e75f05/baus/models.py#L587) step of the developer model.

#### Housing Preservation
* PBA50+ Strategy: H2 "Preserve Existing Affordable Housing".
* `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\preservation_[version].yaml`, specifies geographies and target number of units for preservation.
* Coding notes: the input is read into the model via [def preservation](https://github.com/BayAreaMetro/bayarea_urbansim/blob/1d67ad51a50f46f3d5a7b75b5526420f83c1ccbc/baus/datasources.py#L153); preservation is done via [def preserve_affordable](https://github.com/BayAreaMetro/bayarea_urbansim/blob/ec85eb3f00bd3c2976531612ba14d2252da3224d/baus/subsidies.py#L65).

#### Affordable Housing Subsidy (Housing Bond / Lump-sum Account)
* PAB50+ Strategy: H4 "Build Adequate Affordable Housing to Ensure Homes for All".
* `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\account_strategies_[scenario].yaml`, specifies total funding amount for each county and the target geographies for the subsidy. This assumes a constant "annual funding amount (an independent input)" during the plan period, not considering inflation/deflation over the simulation years.
* Coding notes:
    * The model [reads](https://github.com/BayAreaMetro/bayarea_urbansim/blob/ec85eb3f00bd3c2976531612ba14d2252da3224d/baus/datasources.py#L124) the account_strategies config and sets up [lump_sum_accounts] (https://github.com/BayAreaMetro/bayarea_urbansim/blob/ec85eb3f00bd3c2976531612ba14d2252da3224d/baus/subsidies.py#L166) for each county. For each iteration (5-year), the funding available for each county = annual amount * 5. 
	* In model step [subsidized_residential_developer_lump_sum_accts](https://github.com/BayAreaMetro/bayarea_urbansim/blob/ec85eb3f00bd3c2976531612ba14d2252da3224d/baus/subsidies.py#L906) - [run_subsidized_developer](https://github.com/BayAreaMetro/bayarea_urbansim/blob/ec85eb3f00bd3c2976531612ba14d2252da3224d/baus/subsidies.py#L586C5-L586C29), residential development projects that are not financially feasible ("almost feasible") under market conditions and are within the target geographies may qualify for subsidy. The model ranks these projects by how much per unit subsidy is needed to fill the feasibility gap, then starting from buildings that require the lowest per unit subsidy, draws funding from the corresponding county acount to fill the gap and build deed-restricted units, until either the account is depleted or the demand is met. 
    * Note that in reality, affordable housing finance typical does not work this way. This version of UrbanSim does not have an affordable housing submodel that mimics the funding structure of majority of the 100% affordable housing or mixed-income housing projects.  

#### Housing Development Cost Reduction
* PBA50+ Strategy: H3.
* One way to (indirectly) subsidize housing is to reduce housing development cost reduction, for example, SB743 CEQA reform, lowering parking requirements, etc. This is defined in [profitability_adjustment_policies](https://github.com/BayAreaMetro/bayarea_urbansim/blob/3ecf457e3cf3661992a3a3c5dba126fe1b33db8a/configs/policy.yaml#L1246). The policies are scenario-based, as noted by "enable_in_scenarios". For each policy, [*profitabiity_adjustment_formula*](https://github.com/BayAreaMetro/bayarea_urbansim/blob/98f3b65ea4f29c1f2766659432e8a9d825eed56c/configs/policy.yaml#L1295) picks the parcels in a certain category (e.g. a certain type of geography) and then decreases the required profitability level needed for the model to build on those parcels, e.g. multiplying by 2.5% or 0.025 means to LOWER the required profit level by 2.5%. When a policy has an alternative version is different scenarios, may use 'alternative_geography_scenarios' and 'alternative_adjustment_formula' to consolidate the scenarios.


#### Inclusionary Housing
* PBA50+ Strategy: H5 "Integrate Affordable Housing into All Major Housing Projects".
* `M:\urban_modeling\baus\BAUS Inputs\plan_strategies\inclusionary_strategy_[version].yaml`

#### Strategy Projects (H6,H8,EC2,EC5,EC6)

#### Job Location (EC5)

#### Adapt to Sea Level Rise

