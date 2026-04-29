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
bart_stations.csv| A list of BART stations and their locations so that distance to BART can calculated.
logsums.csv| A set of base year logsums from the travel model.
 
#### travel_model/
**name**|**description**
-----|-----
AccessibilityMarkets_[year].csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.
mandatoryAccessibilities_[year].csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.
nonMandatoryAccessibilities_[year].csv| A travel model output file that incorportates travel model run logsums into the forecast, by year.  

### basis_inputs (in progress)/
#### crosswalks/
**name**|**desription**
-----|-----
[parcel_to_maz22.csv](#parcel_to_maz22csv)| A lookup table from parcels to Travel Model Two MAZs.
[parcel_to_taz1454sub.csv](#parcel_to_taz1454subcsv)| A lookup table from parcels to Travel Model One TAZs.
[parcels_geography.csv](#parcels_geographycsv)| A lookup table from parcels to jurisdiction, growth geographies, UGB areas, greenfield areas, and a concatenation of these used to join these geographies to `zoning_mods.csv`, to apply zoning rules within them.
census_id_to_name.csv| Maps census id from parcels_geography to name so it can be used.
maz_geography| A lookup between MAZ, TAZ2, and county.
maz22_taz1454| A lookup between MAZ and TAZ1.
superdistricts_geography.csv| A map of superdistrict numbers, names, and their subregion.
taz_geography.csv| A lookup between TAZ1, supedisctrict, and county.

#### edits/
**name**|**description**
-----|-----
data_edits.yaml| Settings for editing the input data in the model code, e.g. clipping values. 
manual_edits.csv| Overrides the current h5 data using the table name, attribute name, and new value, so we don't have to generate a new one each time.
household_building_id_overrides.csv| Moves households to match new city household totals during the data preprocessing.
tpp_id_2016.csv| Updates tpp_ids after changes were made to the ids. 

#### existing_policy/
**name**|**description**
-----|-----
development_caps.yaml| Base year job cap policies in place in jurisdictions (TODO: remove the asserted development capsk-factors entangled here.)
inclusionary.yaml| Base year inclusionary zoning policies in place in jurisdictions (TODO: have all model runs inherit these, even if an inclusionary stratey is applied).

#### hazards/
**name**|**desctiption**
-----|-----
slr_progression.csv| The sea level rise level, for each forecast year.
slr_inundation.csv| The sea level rise level at which each inundation parcel becomes inundated, for each forecast year. Rows marked with "100" are parcels where sea level rise has been mitigated, either through planned projects or a plan strategy.

#### parcels_buildings-agents/
**name**|**description**
-----|-----
bayarea_v3.h5| Base year database of households, jobs, buildings, and parcels. The data is pre-processed in pre-processing.py.
costar.csv| Commercial data from CoStar, including non-residential price to inform the price model.
[development_projects.csv](#development_projectscsv)| The list of projects that have happened since the base data, or buildings in the development pipeline. This file tends to have more attributes than we use in the model.
deed_restricted_zone_totals.csv| An approximate number of deed restricted units per TAZ to assign randomly within the TAZ.  
baseyear_taz_controls.csv| Base year control totals by TAZ, to use for checking and refining inputs. The file includes number of units, vacancy rates, and employment by sector (TODO: add households).
sfbay_craisglist.csv| Craigslist data to inform rental unit information and model tenure.

#### zoning/
**name**|**description**
-----|-----
[zoning_parcels.csv](#zoning_parcelscsv)| A lookup table from parcels to zoning_id, zoning area information, and a "nodev" flag (currently all set to 0).
[zoning_lookup.csv](#zoning_lookupcsv)| The existing zoning for each jurisdiction, assigned to parcels with the "id" field. Fields include the city name, city id, and the name of the zoning. The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development.

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
tm2_taz2_forecast_inputs.csv| The same as above, except these inputs provide TAZ2 information, used for Travel Model Two summaries.
tm1_tm2_maz_forecast_inputs.csv| The same as above, except these inputs provide MAZ information, used for btoh Travel Model One and Travel Model Two summaries. 
tm2_emp27_employment_shares| The forecasted share of jobs by 26 sectors, used to apportion that 6 sectors used in the model into more detailed categories Travel Model Two. The shares are provided by county and by year.
tm2_occupation_shares| The forecasted share of jobs by occupation, used for Travel Model Two. The shares are provided by county and by year.
tm1_tm2_regional_controls.csv| Controls from the regional forecast which give us employed residents and the age distribution by year, used to forecast variables used by the travel model.
tm1_tm2_regional_demographic_forecast| Similar to regional_controls.csv, this file provides regional-level information to produce travel model variables, in this case using forecasts of shares by year.

---

## Field Dictionaries

### parcel_to_maz22.csv
A lookup table from parcels to Travel Model Two MAZs.

| variable | type | length | missing_value | null_ok | required | longer_name | description | notes |
|---|---|---|---|---|---|---|---|---|
| OBJECTID_1 | int | 32 | yes | yes | no | | | |
| PARCEL_ID | int | 32 | no | no | yes | Parcel ID for p10 | joins this table to parcels table | |
| APN | string | 63 | yes | yes | no | APN for p10 | | |
| geom_id_s | int | 32 | yes | yes | no | Parcel Geometry ID | as a backup to join this table to parcels table | |
| Min_Shape_Length | float | 32 | yes | yes | no | | | |
| Min_Shape_Area | float | 32 | yes | yes | no | | | |
| maz | int | 32 | no | no | yes | Maz ID | is merged into parcels | |

### parcel_to_taz1454sub.csv
A lookup table from parcels to Travel Model One TAZs.

| variable | type | length | missing_value | null_ok | required | longer_name | description | notes |
|---|---|---|---|---|---|---|---|---|
| OBJECTID | int | 32 | yes | yes | no | | | |
| Join_Count | int | 32 | yes | yes | no | | | |
| TARGET_FID | int | 32 | yes | yes | | | | |
| PARCEL_ID | int | 32 | no | no | yes | Parcel ID for p10 | joins this table to parcels table | |
| ZONE_ID | int | 32 | no | no | yes | TAZ Zone ID | is joined to parcels | |
| manual_county | int | 32 | yes | yes | no | County ID | deprecated | |
| taz_key | int | 28 | yes | yes | no | | | |
| county | string | 52 | yes | yes | no | County Name in 3-letter abbreviation | | |
| subzone | string | 270 | no | no | yes | Subzone ID within Each TAZ | is merged into parcels | |
| taz_sub | string | 54 | no | no | yes | Combined TAZ and Subzone ID | is merged into parcels | |
| Shape_Length | float | 32 | yes | yes | no | | | |
| Shape_Area | float | 32 | yes | yes | no | | | |

### parcels_geography.csv
A lookup table from parcels to jurisdiction, growth geographies, UGB areas, greenfield areas, and a concatenation of these used to join these geographies to `zoning_mods.csv`, to apply zoning rules within them.

| variable | type | length | missing_value | null_ok | required | longer_name | description | notes |
|---|---|---|---|---|---|---|---|---|
| geom_id | int | 32 | | no | yes | Parcel Geometry ID | joins this table to parcel table | |
| PARCEL_ID | int | 28 | | no | no | Parcel ID for p10 | as a backup to join this table to parcel table | |
| jurisdiction_id | int | 28 | | no | yes | PBA50 Jurisdiction FIPS Code | joins "census_id_to_name.csv" to get full jurisdiction name | |
| juris | string | 76 | | no | yes | PBA50 Jurisdiction Full Name | used to generate output | |
| juris_id | string | 5 | | no | no | PBA50 Jurisdiction Name in 4-letter abbreviation | | |
| pda_id | string | 15 | | no | no | PBA40 Priority Development Area ID | | |
| tpp_id | string | 15 | | no | no | PBA40 Transit Priority ID | | |
| exp_id | string | 15 | | no | no | PBA40 Expansion Area ID | | |
| opp_id | string | 15 | | no | no | PBA40 Opportunity Site ID | | |
| zoningmodcat | string | 50 | | no | no | PBA40 Zoning Modification Category | used to join zoningmods tables to parcels | |
| perffoot | int | 28 | | no | no | PBA40 Performance Footprint | | |
| perfarea | int | 28 | | no | no | PBA40 Performance Area | | |
| urbanized | string | 32 | | no | no | PBA40 Urbanized Area | | |
| hra_id | string | 15 | | no | no | Horizon High Resource Area ID | | |
| trich_id | string | 15 | | no | no | Horizon Transit Rich Area ID | | |
| cat_id | string | 45 | | no | no | Horizon Growth Category ID | useful for mapping policy application | |
| zoninghzcat | string | 50 | | no | no | Horizon Zoning Modification Category | used to join zoningmods tables to parcels | |
| gg_id | string | 5 | | no | yes | PBA50 Growth Geography ID | | |
| tra_id | string | 15 | | no | yes | PBA50 Transit Rich Area ID | | |
| sesit_id | string | 10 | | no | yes | PBA50 Socioeconomic Situation ID | whether area is high resource or high displacement risk or both or neither | |
| ppa_id | string | 5 | | no | yes | PBA50 Priority Production Area ID | | |
| exp2020_id | string | 15 | | no | yes | PBA50 Expansion Area ID | | |
| exsfd_id | string | 5 | | no | yes | PBA50 Existing Single Family Home on Parcel ID | | |
| pba50zoningmodcat | string | 70 | | no | yes | PBA50 Zoning Modification Category | used to join zoningmods tables to parcels | |
| nodev | int | 28 | | no | yes | PBA50 No Development Allowed on Parcel | | one of multiple ways to disallow dev |

### development_projects.csv
The list of projects that have happened since the base data, or buildings in the development pipeline. This file tends to have more attributes than we use in the model.

| variable | type | length | missing_value | null_ok | required | longer_name | description | notes |
|---|---|---|---|---|---|---|---|---|
| development_projects_id | int | ? | | no | yes | | | |
| raw_id | int | ? | | yes | no | | | |
| building_name | string | ? | | yes | no | | | |
| site_name | string | ? | | yes | no | | | |
| action | string | ? | | no | yes | | | |
| scen0 | int | short | | no | yes | | | dummy for use in this scenario |
| scen1 | int | short | | no | yes | | | dummy for use in this scenario |
| scen2 | int | short | | no | yes | | | dummy for use in this scenario |
| scen3 | int | short | | no | yes | | | dummy for use in this scenario |
| scen4 | int | short | | no | yes | | | |
| scen5 | int | short | | no | yes | | | dummy for use in this scenario |
| scen6 | int | short | | no | yes | | | dummy for use in this scenario |
| scen7 | int | short | | no | yes | | | dummy for use in this scenario |
| scen10 | int | short | | no | yes | | | dummy for use in this scenario |
| scen11 | int | short | | no | yes | | | dummy for use in this scenario |
| scen12 | int | short | | no | yes | | | dummy for use in this scenario |
| scen15 | int | short | | no | yes | | | dummy for use in this scenario |
| scen20 | int | short | | no | yes | | | dummy for use in this scenario |
| scen21 | int | short | | no | yes | | | dummy for use in this scenario |
| scen22 | int | short | | no | yes | | | dummy for use in this scenario |
| scen23 | int | short | | no | yes | | | dummy for use in this scenario |
| scen24 | int | short | | no | yes | | | dummy for use in this scenario |
| scen25 | int | short | | no | yes | | | dummy for use in this scenario |
| address | string | | | yes | no | | | |
| city | string | | | yes | no | | | |
| zip | string | | | yes | no | | | |
| county | string | | | yes | no | | | |
| x | float | | | no | yes | | | |
| y | float | | | no | yes | | | |
| geom_id | double | | | no | yes | | | |
| year_built | int | short | | no | yes | | | |
| building_type | string | 5 | | yes | yes | | | |
| building_sqft | int | long | | yes | yes | | | |
| non_residential_sqft | int | long | | yes | yes | | | |
| residential_units | int | short | | yes | yes | | | |
| unit_ave_sqft | float | | | yes | no | | | |
| tenure | string | ? | | yes | no | | | |
| rent_type | string | ? | | yes | no | | | |
| stories | int | short | | yes | no | | | |
| parking_spaces | int | short | | yes | no | | | |
| average_weighted_rent | float | ? | | yes | no | | | |
| last_sale_year | int | short | | yes | no | | | |
| last_sale_price | double | ? | | yes | no | | | |
| source | string | | | yes | no | | | |
| PARCEL_ID | int | > | | no | yes | | | |
| ZONE_ID | int | ? | | no | no | | | |
| edit_date | int | long | | yes | no | | | |
| editor | string | ? | | yes | no | | | |

### zoning_parcels.csv
A lookup table from parcels to zoning_id, zoning area information, and a "nodev" flag (currently all set to 0).

| variable | type | length | missing_value | null_ok | required | longer_name | description | notes |
|---|---|---|---|---|---|---|---|---|
| geom_id | int | 32 | | no | yes | Parcel Geometry ID | joins this table to parcels table | |
| PARCEL_ID | int | 28 | | no | no | Parcel ID for p10 | as a backup to join this table to parcels table | |
| zoning_id | int | 28 | | no | yes | Draft Blueprint Base Zoning ID | joins this table to zoning_lookup table | |
| zoning | string | 130 | | no | no | Draft Blueprint Base Zoning Type | | |
| juris | int | 5 | | no | no | PBA50 Jurisdiction FIPS Code | | |
| juris_id | string | 76 | | no | no | PBA50 Jurisdiction Full Name | | |
| prop | int | 24 | | no | no | PBA40 Base Zoning Data Source | | |
| tablename_pba40 | string | 15 | | no | no | PBA40 Opportunity Site ID | | |
| nodev | int | 28 | | no | yes | PBA50 No Development Allowed on Parcel | | one of multiple ways to disallow dev |

### zoning_lookup.csv
The existing zoning for each jurisdiction, assigned to parcels with the "id" field. Fields include the city name, city id, and the name of the zoning. The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development.

| variable | type | length | missing_value | null_ok | required | longer_name | description | notes |
|---|---|---|---|---|---|---|---|---|
| id | int | 28 | no | no | yes | Draft Blueprint Base Zoning ID | joins this table to zoning_parcels table | |
| juris | string | 76 | yes | yes | no | PBA50 Jurisdiction Full Name | | |
| name | string | 130 | yes | yes | no | Draft Blueprint Base Zoning Type | | |
| max_far | float | 32 | yes | yes | yes | Max FAR Allowed | | |
| max_height | float | 32 | yes | yes | yes | Max Height Allowed | | |
| max_dua | float | 32 | yes | yes | yes | Max DUA Allowed | | |
| max_du_per_parcel | float | 32 | yes | yes | no | Max DUA Allowed per Parcel | | |
| HS | int | 28 | yes | yes | yes | SFD Development Allowed | 1 (allow) or 0 (not allow) | |
| HT | int | 28 | yes | yes | yes | Rowhouse Development Allowed | 1 (allow) or 0 (not allow) | |
| HM | int | 28 | yes | yes | yes | MFD Development Allowed | 1 (allow) or 0 (not allow) | |
| OF | int | 28 | yes | yes | yes | Office Development Allowed | 1 (allow) or 0 (not allow) | |
| HO | int | 28 | yes | yes | yes | Hotel Development Allowed | 1 (allow) or 0 (not allow) | |
| SC | int | 28 | yes | yes | yes | School Development Allowed | 1 (allow) or 0 (not allow) | |
| IL | int | 28 | yes | yes | yes | Light Industrial Development Allowed | 1 (allow) or 0 (not allow) | |
| IW | int | 28 | yes | yes | yes | Warehouse Development Allowed | 1 (allow) or 0 (not allow) | |
| IH | int | 28 | yes | yes | yes | Heavy Industrial Development Allowed | 1 (allow) or 0 (not allow) | |
| RS | int | 28 | yes | yes | yes | Retail Development Allowed | 1 (allow) or 0 (not allow) | |
| RB | int | 28 | yes | yes | yes | Big Box Retail Development Allowed | 1 (allow) or 0 (not allow) | |
| MR | int | 28 | yes | yes | yes | Mixed Residential Development Allowed | 1 (allow) or 0 (not allow) | |
| MT | int | 28 | yes | yes | yes | Mixed Retail Development Allowed | 1 (allow) or 0 (not allow) | |
| ME | int | 28 | yes | yes | yes | Mixed Employment Focused Development Allowed | 1 (allow) or 0 (not allow) | |