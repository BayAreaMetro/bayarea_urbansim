from __future__ import print_function
import orca
import pandas as pd
import numpy as np


@orca.step()
def preserve_affordable(preservation_policy, year, base_year, residential_units,
                        taz_geography, buildings, parcels_geography):


    # join several geography columns to units table so that we can apply units
    res_units = residential_units.to_frame()
    bldgs = buildings.to_frame()
    parcels_geog = parcels_geography.to_frame()
    taz_geog = taz_geography.to_frame()

    res_units = res_units.merge(bldgs[['parcel_id']], left_on='building_id', right_index=True, how='left').\
                          merge(parcels_geog[['gg_id', 'sesit_id', 'tra_id', 'juris']], left_on='parcel_id', 
                                right_index=True, how='left').\
                          merge(taz_geog, left_on='zone_id', right_index=True, how='left')

    # only preserve units that are not already deed-restricted
    res_units = res_units.loc[res_units.deed_restricted != 1]
    # initialize list of units to mark deed restricted
    dr_units = []
    
    # apply deed-restriced units by filter within each geography 
    for i in preservation_policy: 
        
        if preservation_policy[i].unit_target is None
        continue
         
        unit_target = preservation_policy[i].unit_target

        # exclude units that have been preserved through this loop
        res_units = res_units[~res_units.index.isin(dr_units)]

        # subset units to the geography and filter area
        geog_units = res_units.loc[res_units[geography] == preservation_policy[i].geography]
        filter_units = geog_units.query(preservation_policy[i].unit_filter)

        # pull a random set of units based on the target except in cases
        # where there aren't enough units in the filtered geography or
        # they're already marked as deed restricted
        if len(filter_units) == 0:
            dr_units_set = []
        elif unit_target > len(filter_units):
            dr_units_set = filter_units.index
        else:
            dr_units_set = np.random.choice(filter_units.index, 
                                            unit_target, replace=False)

        dr_units.extend(dr_units_set)

    # mark units as deed restriced in residential units table
    residential_units = residential_units.to_frame()
    residential_units.loc[residential_units.index.isin(dr_units), 
                          'deed_restricted'] = 1
    orca.add_table("residential_units", residential_units)

    # mark units as deed restricted in buildings table
    buildings = buildings.to_frame(buildings.local_columns)
    new_dr_res_units = residential_units.building_id.loc[residential_units.\
        index.isin(dr_units)].value_counts()
    buildings["preserved_units"] = (buildings["preserved_units"] + 
        buildings.index.map(new_dr_res_units).fillna(0.0))     
    buildings["deed_restricted_units"] = (buildings["deed_restricted_units"] + 
        buildings.index.map(new_dr_res_units).fillna(0.0))
    orca.add_table("buildings", buildings)