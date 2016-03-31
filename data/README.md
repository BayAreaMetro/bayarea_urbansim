A few notes on the zoning_lookup, which hopefully makes this an evolving document.

The zoning lookup is the current, BASELINE zoning for each jurisdiction, and can be assigned to parcels.  The lookup is identified by the unique id ("id" column), has the city name, city id, and the name of the zoning.

The active attributes are max_dua, max_far, and max_height, all of which must be respected by each development, so this means the most constraining constraint has the power.  dua is "Dwelling Units per Acre", far is "Floor Area Ratio" (ratio of square footage in building to square footage on parcel), and height is... height. 

This means there *must* be an equivlance between dua and far if they are both present.  This is tricky, and requires knowing things like average unit sizes, as well as net to gross factors for the unit square footage.  We make our best guesses on this, but this logic can be configured.  There also must be an equivalance between far and height, which generally is made by taking far and multiplying is by a net to gross building footprint factor (e.g. a calculation to figure out the height of a building with FAR 4.0, when the footprint of the building takes up 80% or so of the parcel, when stories are all 12 feet, and where partial stories count as full stories in terms of height).  As you are thinking at this point, we are bad building architects, but we're making rough approximations here in order to allocate built space in the region and this feels like the appropriate level of detail.

The other columns are are building types, and the values are 0 or 1 to indicate whether the type is allowed on this parcel.  For instance, HS is single family, HT is townhome (attached), and HM is multi-family (apartments / condos) and so forth.  R for retail, OF is office, and M is mixed.

max_du_per_parcel is not currently used because we don't necessarily trust our data, but in theory is used to say, e.g., that only a single unit can be built on each parcel.

Note that a blank value in the csv means that there is no constraint for that attribute and the other constraints will be used.  If there is no constraint for any of far, height, and dua, a building will NOT be built. 

*By convention a 0 in dua, far, and height is discouraged.  It is preferred to use the "building type not allowed" columns for this*

A few other notes on the addition of development projects. 

1. We use two different approach to join the project list to the parcel database in order to get the GEOM_ID, which is important in the model run.
	(1) If local jurisdictions provide Assessor's Parcel Number (APN) for the project, then APN is used in the table join process.
	(2) Otherwise, we map out the projects using the addresses provided by jurisdictions and then join the parcel database spatially. However, many times addresses provided by jurisdictions would map onto an intersection or street instead of onto a parcel. In those cases, we use an aerial map to choose a location that is deemed possible for the project.
  In the mapping process, sometimes method (2) would make more sense than method (1) when looking at the scale of the projects and the size of the parcel. However, for now we will use APN as the primary joining method despite the situations described.
 
2. There could be cases where parcel consolidation has happened for large scale projects after 2010, but because the parcel database is from 2010, it is possible that projects would be mapped to small parcels from before the the parcel consolidation. 

3. "Year_built" is also an important variable for the model run, however, local jurisdiction may not provide this information. For the added projects, value for the "year_built" variable could be: (1) year when the projects were approved; (2) year when the projects started to be built; (3) year when the projects were completed; (4) year of the development activity reported was published; But not the year when the projects are anticipated to be completed.

4) Two other columns are required for the project to be used by UrbanSim.  The "action" field must be set to "build" or "add" - "build" will demolish current buildings while "add" will not.  Second, the building type must be set.  The set of building types that are available is defined is [settings](https://github.com/MetropolitanTransportationCommission/bayarea_urbansim/blob/master/configs/settings.yaml).

