### BAUS Inputs
The inputs structure for BAUS and a description of each input. Model input files are stored in an `inputs` folder to be called by the model. They are often run-specific and contain the data used to run the model, such as base year datasets and policy inputs.

### inputs/

### accessibility/
#### pandana/
**name**|**description**
-----|-----
tmnet.h5| Travel model network information for calculating accessibility within the model using Pandana
osm_bayarea4326.h5| Street network information for calculating accessibility within the model using Pandana
landmarks.csv| Locations of a few major landmarks in the region for accessibility calculations.
regional_poi_distances.csv| The pre-computed distances from each travel model node to each landmark. 
bart_stations_2020.csv| A list of BART stations and their locations so that distance to BART can calculated. 
#### travel_model/
**name**|**description**
-----|-----
AccessibilityMarkets_[year].csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.
mandatoryAccessibilities_[year].csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.
nonMandatoryAccessibilities_[year].csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.  

### basis_inputs/
#### crosswalks/
**name**|**desription**
-----|-----
growth_geographies.csv| A lookup table from parcels to growth geographies, zoning mod categories (see: plan_strategies/), and UGB zoning mod categories.
travel_model_geographies.csv| A lookup between TAZ, supedisctrict, and subgregion.

#### equity/
**name**|**description**
-----|-----

#### existing_policy/
**name**|**description**
-----|-----
development_caps.yaml| Base year job cap policies in place in jurisdictions (TODO: remove the asserted development caps k-factors entangled here.)
inclusionary.yaml| Base year inclusionary zoning policies in place in jurisdictions (TODO: have all model runs inherit these, even if an inclusionary stratey is applied).
boc.csv | Base year build out capacity (zoning) information for each parcel, including max_dua, max_far, and max_height, all of which must be respected by each development. 

#### hazards/
**name**|**desctiption**
-----|-----
slr_progression.csv| The sea level rise level for each forecast year.
slr_inundation.csv| The sea level rise level at which each inundation parcel becomes inundated.

#### parcels_buildings-agents/
**name**|**description**
-----|-----
parcels.csv| A list of parcels in the region, their jurisdction and county.
buildings.csv| A list of buildings in the region, which link to their parcel, and select building attributes.
residential_units.csv| This reflects the same information as the buildings table, but creates a row for each unit in a building for the model to use.
jobs.csv| A list of all jobs in the region and their industry category. Each job has an associated building ID.
households.csv| A list of all households in the region and their income category. Each household has an associated residential unit ID.
core_datasets.h5| This file simply packages the above datasets for use during model runtime.
development_projects.csv| The list of projects that have happened since the base data, or buildings in the development pipeline.
institutions.csv| These are job locations in the region that operate outside of the commerical real estate market, therefore are set off-limits for development and for jobs to relocate from.
nodev_sites.csv| This is a list of all sites set off-limits for development with their "nodev" category, including uses such as open space and historic buildings.
craisglist.csv| Craigslist rental data use for model estimation.  
costar.csv| Commercial building data usd for model estimation.

### plan_strategies (optional)/
**name**|**description**
-----|-----
accessory_units.csv| A file to add accessory dwelling units to jurisdictions by year, simulating policy to allow or reduce barriers to ADU construction in jurisdictions (TODO: Make this a default policy).
account_strategies.yaml| This files contains the settings for all strategies in a model run that use accounts. The file may include account settings (e.g., how much to spend, where to spend) for| housing development bonds, office development bonds, OBAG funds, and VMT fees (which collect fees and then can spend the subsidies). 
development_caps_strategy.yaml| A file that specifies a strategy to limit development (generally office development) to a certain number of residential units andor job spaces.
inclusionary_strategy.yaml| A file to apply an inclusionary zoning strategy by geography and inclusionary housing requirement percentage.
preservation.yaml| A file to apply an affordable housing preservation strategy through specifying geography and target number of units for preservation. 
profit_adjustment_stratgies.yaml| This file contains the settings for all strategies in a model run which modify the profitability of projects thus altering their feasibility. The file may include profit adjustment settings (e.g., the percent change to profit) for| development streamlining, CEQA streamlining, parking requirements reductions, and profitability changes from SB-743.
renter_protections_relocation_rates_overwrites| The rows in this file overwrite the household relocation rates in the model's settings.
telecommute_sqft_per_job_adjusters| These are multipliers which adjust the sqft per job setting by superdistrict by year to represent changes from a telework strategy. (TODO: Disentangle the k-factors and the policy application within this file and sqft_per_job_adjusters.csv. In the meantime, use both files as is done in the PBA50 No Project).
vmt_fee_zonecats.csv| This file pairs with the VMT Fee and SB-743 strategies. It provides VMT levels by TAZ1, which map to the corresponding price adjustments in the strategies.
zoning_mods.csv| A file which allows you to upzone or downzone. If you enter a value in "dua_up" or "far_up", the model will apply that as the new zoning or maintain the existing zoning if it is higher. If you enter a value in "dua_down" or "far_down", the model will apply that as the zoning or maintain the existing zoning if it is lower. UGBs are also controlled using this file, using zoning changes to enforce them. This file is mapped to parcels using the field "zoningmodcat", which is the concatenated field of growth designations in parcels_geography.csv.
  
### regional_controls/ 
**name**|**description**
-----|-----
employment_controls.csv| The total number of jobs in the region for the model to allocate, by year. The controls are provided by 6-sector job category.
household_controls.csv| The total number of households in the region for the model to allocate, by year. The controls are provided by household income quartile.

### zone_forecasts/
**name**|**description**
-----|-----
taz_growth_rates_gov_ed.csv| This file has ratios of governement and education employment per population by County and TAZ. The files has two header rows| the first row is what the outcome attribute is and the second is the geography at which the ratio acts (either TAZ, County, or Regional).
prportional_retail_jobs_forecast.csv| This contains the field "minimum_forecast_retail_jobs_per_household" by jurisdiction, which is used to keep local numbers of retail jobs reasonable through the forecast.
tm1_taz1_forecast_inputs.csv| This is closely related to regional_controls.csv. These are zone level inputs used for the process of generating variables for the travel model, while the other file contains regional-level controls. These inputs provide TAZ1454 information, used for Travel Model One summaries. 
tm2_taz2_forecast_inputs.csv| The same as above, except these inputs provide TAZ2 information, usED for Travel Model Two summaries.
tm1_tm2_maz_forecast_inputs.csv| The same as above, except these inputs provide MAZ information, used for btoh Travel Model One and Travel Model Two summaries. 
tm2_emp27_employment_shares| The forecasted share of jobs by 26 sectors, used to apportion that 6 sectors used in the model into more detailed categories Travel Model Two. The shares are provided by county and by year.
tm2_occupation_shares| The forecasted share of jobs by occupation, used for Travel Model Two. The shares are provided by county and by year.
tm1_tm2_regional_controls.csv| Controls from the regional forecast which give us employed residents and the age distribution by year, used to forecast variables used by the travel model.
tm1_tm2_regional_demographic_forecast| Similar to regional_controls.csv, this file provides regional-level information to produce travel model variables, in this case using forecasts of shares by year.