#!/usr/bin/env python
# coding: utf-8

import os
import sys
import logging
import yaml
from datetime import datetime
from pathlib import Path

import pandas as pd
import geopandas as gpd

from transit_area_buffer_utils import (
    create_multiple_transit_areas,
    # data_filtering,
    # get_combination_names,
    # get_combination_names_hierarchy,
    # station_buffer,
    col_values,
)

# DVUTILS_LOCAL_CLONE_PATH = "/Users/aolsen/Documents/GitHub/dvutils"
# sys.path.insert(0, DVUTILS_LOCAL_CLONE_PATH)
# from utils_io import *

user = os.getlogin()

# ## Paths
# ### Practical

home_dir = Path.home()
box_dir = Path(home_dir, "Box")

# set the mounted path for M: drive
# in my (AO) case, M:\urban_modeling is mounted to /Volumes/Data/Models/urban_modeling
m_drive = "/Volumes/Data/Models" if os.name != "nt" else "M:/"

metrics_path = Path(
    box_dir,
    "Plan Bay Area 2050+",
    "Performance and Equity",
    "Plan Performance",
    "Equity_Performance_Metrics",
    "PBA50_reproduce_for_QA",
)

# working_dir_path = os.path.abspath(".")
# M has been unstable recently, so heading to Box for now.
working_dir_path = Path(
    m_drive, "Data", "GIS layers", "JobsHousingTransitProximity", "update_2024"
)

working_dir_path = Path(
    box_dir,
    "Modeling and Surveys",
    "Urban Modeling",
    "Spatial",
    "transit",
    "transit_service_levels",
    "update_2024",
)
# working_dir_path.mkdir(parents=True, exist_ok=True)

YMD = datetime.now().strftime("%Y%m%d")
today_path = Path(working_dir_path, "outputs", YMD)
today_path.mkdir(parents=True, exist_ok=True)

# boolean for whether the main parcel to service area crosswalk already exists
# crosswalk_exists = parcel_classes_gpd_path.exists()


def logger_process(dir_path: Path, log_name: str = "proximity2transit.log"):

    # Create a unique log file path
    log_file = Path(dir_path, log_name)

    # Create the output directory if it doesn't exist
    # output_dir = Path(dir_path, "outputs")
    # output_dir.mkdir(parents=True, exist_ok=True)  # Create parent directories if needed

    # Configure logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Sset format for log messages
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )

    # Console handler with INFO level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler with DEBUG level
    fh = logging.FileHandler(log_file, mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info(f"Initializing log file at {log_file.as_posix()}")

    return logger


# ### Data paths - AGOL URLs, and local versions until we get utils working
MTC_ONLINE_TRANSIT_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/transitstops_existing_planned_2021/FeatureServer/0"

MTC_ONLINE_BACOUNTY_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/region_county/FeatureServer/0"

MTC_ONLINE_TAZ_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/transportation_analysis_zones_1454/FeatureServer/0"

MTC_ONLINE_TRACT_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/cocs_ACS2014_2018/FeatureServer/0"

# # create arc client object
# arcgis_client = create_arcgis_client()

# # pull data
# transit_data = pull_geotable_agol(base_url=MTC_ONLINE_TRANSIT_URL, client=arcgis_client)

# Define paths to data files
# TODO: a bunch of these could be fetched from live sources such as AGOL. Currently getting token errors, so
# leaving local versions active here

tract_epc21_path = Path(
    m_drive, "Data", "Equity Priority Communities", "PBA50", "EPCs_ACS2018_tbl.csv"
)
tract_epc24_path = Path(
    m_drive, "Data", "Equity Priority Communities", "PBA50Plus", "epc_acs2022.csv"
)

# extracted from AGOL around March 20, 2024
tract_epc_path = Path(
    m_drive,
    "Data",
    "GIS layers",
    "JobsHousingTransitProximity",
    "update_2024",
    "inputs",
    "proximity",
    "communities_of_concern_2020_acs2018_-5728172521147435275.gpkg",
)
# # extracted from AGOL around March 20, 2024
# county_path = Path(
#     m_drive,
#     "Data",
#     "GIS layers",
#     "JobsHousingTransitProximity",
#     "update_2024",
#     "inputs",
#     "proximity",
#     "region_county_-6301486166122500334.gpkg",
# )
# extracted from AGOL around March 20, 2024
transit_path = Path(
    m_drive,
    "Data",
    "GIS layers",
    "JobsHousingTransitProximity",
    "update_2024",
    "inputs",
    "proximity",
    "transitstops_existing_planned_2021_2777787894694541529.gpkg",
)
# extracted from AGOL around March 20, 2024
taz_path = Path(
    m_drive,
    "Data",
    "GIS layers",
    "JobsHousingTransitProximity",
    "update_2024",
    "inputs",
    "proximity",
    "transportation_analysis_zones_1454_5961535026220395633.gpkg",
)
tract_hra_2023_path = Path(m_drive, "Data", "HCD", "final_2023_public.xlsx")
tract_hra_2019_path = Path(
    m_drive, "Data", "HCD", "final-opportunity-map-statewide-summary-table.xlsx"
)

# Tracts
tract_2020_path = Path(
    m_drive, "Data", "GIS layers", "Census", "2020", "censustracts_bayarea_2020_v2.shp"
)
tract_2010_path = Path(
    m_drive, "Data", "GIS layers", "Census", "2010", "tracts_bayarea_2010_geoid.shp"
)

# Set up crosswalks for parcels to something else

parcel_x_taz_file = Path(
    m_drive,
    "urban_modeling",
    "baus",
    "BAUS Inputs",
    "basis_inputs",
    "crosswalks",
    "2020_08_17_parcel_to_taz1454sub.csv",
)
parcel_x_tracts_file = Path(
    m_drive,
    "urban_modeling",
    "baus",
    "BAUS Inputs",
    "basis_inputs",
    "crosswalks",
    "p10_census.csv",
)
parcel_topo_file = Path(
    box_dir,
    "Modeling and Surveys",
    "Urban Modeling",
    "Bay Area UrbanSim",
    "PBA50",
    "Policies",
    "Base zoning",
    "inputs",
    "p10_geo.feather",
)
taz_x_areatype_file = Path(
    m_drive,
    "Application",
    "Model One",
    "RTP2021",
    "Blueprint",
    "INPUT_DEVELOPMENT",
    "metrics",
    "metrics_FinalBlueprint",
    "taz_urban_suburban.csv",
)

ANALYSIS_CRS = "EPSG:26910"
# ## Constants and mappings

# TODO: should probably not be in global name space
# transit_buffers = {}


def assign_transit_service_areas_to_parcels(
    parcels_geo, transit_areas, slug="current", variable="cat5"
):
    """
    Assigns transit service areas to parcels based on point in poly.

    This function takes two GeoDataFrames, one representing parcels and another representing
    transit service areas, and assigns each parcel to the containing transit area.

    Args:
      parcels_geo (GeoDataFrame): A GeoDataFrame containing the parcels data.
      transit_areas (GeoDataFrame): A GeoDataFrame containing the transit areas data.
      slug (str, optional): A slug to identify the type of transit areas being used. Defaults to 'current'.
      variable (str, optional): The variable name in the transit areas DataFrame representing the transit service area category.
                                 Defaults to 'cat5'.

    Returns:
      pandas.Series: A pandas Series containing the assigned transit service area category for each parcel, indexed by the parcel ID.

    """

    logger.info(f"Joining transit areas ({slug}) to parcels")

    # core assignment, per sjoin
    p10_x_transit_areas = gpd.sjoin(parcels_geo, transit_areas, predicate="within")

    # check for unassigned
    unassigned_parcel_ids = set(parcels_geo.PARCEL_ID) - set(
        p10_x_transit_areas.PARCEL_ID
    )

    logger.info(
        f"There were {len(unassigned_parcel_ids)} parcels without an assignment to a transit area.\nWe assign to nearest area."
    )

    # second pass - for any unassigned parcels - associate based on nearest instead of within
    p10_x_transit_areas_remainder = gpd.sjoin_nearest(
        parcels_geo.query("PARCEL_ID.isin(@unassigned_parcel_ids)"), transit_areas
    )

    # combine the two
    combo_assignments = pd.concat([p10_x_transit_areas, p10_x_transit_areas_remainder])

    if (combo_assignments.PARCEL_ID.value_counts().sort_values() > 1).any():

        # if there are parcel duplicates - get the fist classification
        mapping = combo_assignments.groupby("PARCEL_ID")[variable].first()
    else:
        # return the original mapping

        mapping = combo_assignments.set_index("PARCEL_ID")[variable]

    # store in global namespace dict
    transit_scenario_crosswalk[f"{slug}_{variable}"] = mapping

    xwalk_path = Path(today_path, f"{slug}_{variable}.csv")
    logger.info(f"Writing crosswalk to {xwalk_path}")
    mapping.reset_index().to_csv(xwalk_path)

    return mapping


# This could perhaps be handled by more general lookups from DataViz instead
# of rolling our own on the fly. It's a kind of grab bag crosswalk with
# often multiple vintages (e.g. EPCs, HRAs for different years). A more
# targeted crosswalk could perhaps be more useful / efficient. But this
# makes it easier to adjust grouping levels when calculating metrics.


def add_classifications_to_p10_topo(p10_topofix_pt):
    """
    Adds classifications to parcels based on the assigned transit service area.

    This function takes a GeoDataFrame of parcels and adds classifications based on the
    assigned transit service area. The classifications are added to the GeoDataFrame as
    new columns.

    Args:
      p10_topofix_pt (GeoDataFrame): A GeoDataFrame containing the parcels data.

    Returns:
      GeoDataFrame: A GeoDataFrame with the classifications added as new columns.

    """

    #######################################################################
    # Load data ###########################################################
    #######################################################################

    logger.info("Begin loading data")

    # load a feather version of p10 parcels after the topology fixes
    # https://app.asana.com/0/1202179257399094/1205973096797373/f

    logger.info("\tLoading hras 2023")
    tract_hra_2023 = pd.read_excel(
        tract_hra_2023_path, "BayArea", dtype={"Census Tract": str}
    ).rename(columns={"Census Tract": "geoid", "Final Category": "hra_category"})
    tract_hra_2023.hra_category = tract_hra_2023.hra_category.fillna("Missing")

    logger.info("\tLoading hras 2019")
    tract_hra_2019 = pd.read_excel(
        tract_hra_2019_path, "BayArea", dtype={"Tract FIPS Code": str}
    ).rename(columns={"Tract FIPS Code": "geoid", "Final Category": "hra_category"})
    tract_hra_2019.hra_category = tract_hra_2019.hra_category.fillna("Missing")

    # Load EPC tract files
    logger.info("\tLoading epc 2021")
    tract_epc21_file = pd.read_csv(tract_epc21_path)

    logger.info("\tLoading epc 2024")
    tract_epc24_file = pd.read_csv(tract_epc24_path)

    # # Load county gdf
    # logger.info("\tLoading county geoms")
    # county_file = gpd.read_file(county_path)
    # county_file = county_file.to_crs(ANALYSIS_CRS)

    # load TAZ boundaries
    logger.info("\tLoading taz geoms")
    taz_file = gpd.read_file(taz_path)
    taz_file = taz_file.to_crs(ANALYSIS_CRS)

    logger.info("\tLoading tract 2010 geoms")
    tract_2010_file = gpd.read_file(tract_2010_path, engine="pyogrio")
    logger.info("\tLoading tract 2020 geoms")
    tract_2020_file = gpd.read_file(tract_2020_path, engine="pyogrio")

    # Enforce 11-character tract GEOID

    logger.info("Enforcing 11-character tract ids")
    tract_epc21_file["geoid10"] = tract_epc21_file.geoid.map(lambda x: f"{x:011d}")
    tract_epc24_file["geoid20"] = tract_epc24_file.tract_geoid.map(
        lambda x: f"{x:011d}"
    )

    # ### Crosswalks

    # Load parcel-to-taz crosswalk
    logger.info("Loading crosswalks")
    logger.info("\tLoad parcel-to-taz crosswalk")
    parcel_x_taz = pd.read_csv(parcel_x_taz_file, usecols=["PARCEL_ID", "ZONE_ID"])

    # load parcel-to-tract10, tract20 crosswalk
    logger.info("\tLoad parcel-to-tracts crosswalk")
    parcel_x_tracts = pd.read_csv(parcel_x_tracts_file)

    # load parcel-to-areatype crosswalk
    logger.info("\tLoad taz-to-areatype crosswalk")
    taz_x_areatype = pd.read_csv(taz_x_areatype_file, index_col=["TAZ1454"]).area_type

    logger.info("Adding classifications to parcels based on transit service area")

    # get the transit service area mapping
    # taz_x_areatype = taz_x_areatype_mapping.set_index("taz")["area_type"]

    # get the epc (RTP2021 version) to parcels using tract10
    # tract_epc21_file = tract_epc21_file.set_index("geoid10")["epc_class"]

    # get the epc (RTP2021 version) to parcels using tract20

    ####################################################
    # add tractids, tazs to topo parcels

    # vintage 2010
    p10_topofix_pt["tract10"] = p10_topofix_pt.PARCEL_ID.map(
        parcel_x_tracts.set_index("parcel_id").tract10.map(lambda x: f"{x:011.0f}")
    )
    col_values(p10_topofix_pt["tract10"], "tract10", logger)

    # vintage 2020
    p10_topofix_pt["tract20"] = p10_topofix_pt.PARCEL_ID.map(
        parcel_x_tracts.set_index("parcel_id").tract20.map(lambda x: f"{x:011.0f}")
    )
    col_values(p10_topofix_pt["tract20"], "tract20", logger)

    # add TAZs
    p10_topofix_pt["taz"] = p10_topofix_pt.PARCEL_ID.map(
        parcel_x_taz.set_index("PARCEL_ID").ZONE_ID
    )
    col_values(p10_topofix_pt["taz"], "taz", logger)

    ####################################################

    # add area type based on the just assigned taz
    p10_topofix_pt["area_type"] = p10_topofix_pt.taz.map(taz_x_areatype)
    col_values(p10_topofix_pt["area_type"], "area_type", logger)

    # add epc (RTP2021 version) to parcels using tract10
    p10_topofix_pt["epc21"] = p10_topofix_pt.tract10.map(
        tract_epc21_file.set_index("geoid10").epc_class.fillna("Not EPC")
    )
    col_values(p10_topofix_pt["epc21"], "epc21", logger)

    p10_topofix_pt["is_epc21"] = (p10_topofix_pt.epc21 != "Not EPC").map(
        {True: "CoCs", False: "Not CoCs"}
    )
    col_values(p10_topofix_pt["is_epc21"], "is_epc21", logger)

    # add epc (RTP2025 version) to parcels using tract10
    p10_topofix_pt["epc24"] = p10_topofix_pt.tract20.map(
        tract_epc24_file.set_index("geoid20").epc_class.fillna("Not EPC")
    )
    col_values(p10_topofix_pt["epc24"], "epc24", logger)

    p10_topofix_pt["is_epc24"] = (p10_topofix_pt.epc24 != "Not EPC").map(
        {True: "CoCs", False: "Not CoCs"}
    )
    col_values(p10_topofix_pt["is_epc24"], "is_epc24", logger)

    # add hra23 to parcels using tract10 (!)
    p10_topofix_pt["hra23"] = p10_topofix_pt.tract10.map(
        tract_hra_2023.set_index("geoid").hra_category
    )
    col_values(p10_topofix_pt["hra23"], "hra23", logger)

    p10_topofix_pt["is_hra23"] = p10_topofix_pt.hra23.isin(
        ["High Resource", "Highest Resource"]
    ).map({True: "HRAs", False: "Not HRAs"})
    col_values(p10_topofix_pt["is_hra23"], "is_hra23", logger)

    # add hra19 to parcels using tract10
    p10_topofix_pt["hra19"] = p10_topofix_pt.tract10.map(
        tract_hra_2019.set_index("geoid").hra_category
    )
    col_values(p10_topofix_pt["hra19"], "hra19", logger)

    p10_topofix_pt["is_hra19"] = p10_topofix_pt.hra19.isin(
        ["High Resource", "Highest Resource"]
    ).map({True: "HRAs", False: "Not HRAs"})
    col_values(p10_topofix_pt["is_hra19"], "is_hra19", logger)

    # assign different service level resolution classifications to parcels
    for key, val in transit_scenario_crosswalk.items():
        service_level_var = f"Service_Level_{key}"

        p10_topofix_pt[service_level_var] = p10_topofix_pt.PARCEL_ID.map(
            transit_scenario_crosswalk[key]
        )
        col_values(p10_topofix_pt[service_level_var], service_level_var, logger)

    return p10_topofix_pt


if __name__ == "__main__":

    RUN_5_WAY_BUFFERS = True
    RUN_6_WAY_BUFFERS = True
    RUN_2_WAY_BUFFERS = True

    logger = logger_process(working_dir_path, f"proximity2transit_{YMD}.log")

    #######################################################################
    # Begin processing ####################################################
    #######################################################################

    # load transit stops gdf
    logger.info("\tLoading transit stops")
    transit_file = gpd.read_file(transit_path)
    transit_file = transit_file.to_crs(ANALYSIS_CRS)

    # Create separate stop universes: existing; under construction; open; or FBP

    logger.info("Segmenting transit data into current; current plus FBP, current plus NP")
    logger.info(f"\tRecords in raw transit stop data: {len(transit_file)}")

    qry_fbp = (
        'status.isin(["Under Construction","Open","Final Blueprint","Existing/Built"]) '
    )

    transit_fbp = transit_file.query(qry_fbp)
    logger.info(
        f"\tSubsetting transit data for Final Blueprint with: {qry_fbp}\nRecords after: {len(transit_fbp)}"
    )

    # subset to existing major stop, and under construction or open
    # | (major_stop==1 & status!="Final Blueprint")
    qry_np = 'status.isin(["Under Construction","Open","Existing/Built"]) '

    transit_np = transit_file.query(qry_np)
    logger.info(
        f"\tSubsetting transit data for No Project with: {qry_np}\nRecords after: {len(transit_np)}"
    )

    # subset to existing transit infrastructure (per 2015 or so, pre-plan)

    qry_current = ' status=="Existing/Built"'

    transit_current = transit_file.query(qry_current)

    logger.info(
        f"\tSubsetting transit data for Current Conditions with: {qry_current}\nRecords after: {len(transit_current)}"
    )

    logger.info("\tLoad parcel geoms (pre-processed to feather, which loads in 10s)")
    p10_topofix = gpd.read_feather(parcel_topo_file)
    p10_topofix["geom_pt"] = p10_topofix.representative_point()
    p10_topofix_pt = p10_topofix.set_geometry("geom_pt")[["PARCEL_ID", "geom_pt"]]

    # store parcel-to-transit service area crosswalks of different stripes
    transit_scenario_crosswalk = {}

    # ## Step 1a: define filters for buffers and headways on transit stops - each category is defined separately, and we combine in a list
    # these are in effect the parameters for the buffers: headways and buffer sizes.
    # set specific criteria for selecting headways and what kind of buffer to apply to selected stops

    if RUN_5_WAY_BUFFERS:
        # these constants are sort of post-hoc - the categories (here, 5-way) is what they are because we define four categories, plus rest of region
        # if we had a different set of criteria define here, we would have a different set of constants


        # define filter dicts for identifying the proper stops and how much to buffer them
        filter_criteria_major_stop = {
            "name": "Major_Transit_Stop",
            "buffer": 0.5,
            "major_stop": [{"operator": "==", "value": 1}],
        }

        filter_criteria_hdwy_lt15 = {
            "name": "Bus_<15min",
            "buffer": 0.25,
            "am_av_hdwy": [{"operator": "<=", "value": 15}],
            "pm_av_hdwy": [{"operator": "<=", "value": 15}],
        }

        filter_criteria_hdwy_15_to_30 = {
            "name": "Bus_15_30min",
            "buffer": 0.25,
            "am_av_hdwy": [
                {"operator": ">", "value": 15},
                {"operator": "<=", "value": 30},
            ],
            "pm_av_hdwy": [
                {"operator": ">", "value": 15},
                {"operator": "<=", "value": 30},
            ],
        }

        filter_criteria_hdwy_gt30 = {
            "name": "Bus_30plusmin",
            "buffer": 0.25,
            "am_av_hdwy": [{"operator": ">", "value": 30}],
            "pm_av_hdwy": [{"operator": ">", "value": 30}],
        }

        criteria_list_cat5 = [
            filter_criteria_major_stop,
            filter_criteria_hdwy_lt15,
            filter_criteria_hdwy_15_to_30,
            filter_criteria_hdwy_gt30,
        ]

        # Next, use this list to create transit areas with *5-way* (including remainder areas) categorization

        #TODO: consider just looping create_multiple_transit_areas() and in turn assign_transit_service_areas_to_parcels() as this gets repetitive when needing to run for multiple resolutions
        # using current transit service
        transit_areas_current_cat5, transit_areas_current_dissolved_cat5 = (
            create_multiple_transit_areas(
                transit_current,
                criteria_list_cat5,
                "current_cat5",
                True,
                today_path,
                logger=logger,
            )
        )

        # using current plus fbp transit service
        transit_areas_fbp_cat5, transit_areas_fbp_dissolved_cat5 = (
            create_multiple_transit_areas(
                transit_fbp, 
                criteria_list_cat5, 
                "fbp_cat5", 
                True, 
                today_path, 
                logger=logger
            )
        )

        # using current plus np transit service
        transit_areas_np_cat5, transit_areas_np_dissolved_cat5 = (
            create_multiple_transit_areas(
                transit_np, 
                criteria_list_cat5, 
                "np_cat5", 
                True, 
                today_path, 
                logger=logger
            )
        )

        # #### cat5 - 5 way categorization assignment to parcels, for each stop universe (np, fbp, current)
        p10_x_transit_area_np_cat5 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_np_dissolved_cat5,
            slug="np",
            variable="cat5",
        )

        p10_x_transit_area_fbp_cat5 = assign_transit_service_areas_to_parcels(
            p10_topofix_pt,
            transit_areas=transit_areas_fbp_dissolved_cat5,
            slug="fbp",
            variable="cat5",
        )

        p10_x_transit_area_current_cat5 = assign_transit_service_areas_to_parcels(
            p10_topofix_pt,
            transit_areas=transit_areas_current_dissolved_cat5,
            slug="current",
            variable="cat5",
        )

    if RUN_6_WAY_BUFFERS:

        ###########################################################################
        # Repeat the exercise, but for a 6-way categorization
        # set criteria for selecting headways and what kind of buffer to apply to selected stops
        # Finer cut

        filter_criteria_major_stop = {
            "name": "Major_Transit_Stop",
            "buffer": 0.5,
            "major_stop": [{"operator": "==", "value": 1}],
        }

        filter_criteria_hdwy_lt10 = {
            "name": "Bus_<10min",
            "buffer": 0.25,
            "am_av_hdwy": [{"operator": "<=", "value": 10}],
            "pm_av_hdwy": [{"operator": "<=", "value": 10}],
        }

        filter_criteria_hdwy_11_to_15 = {
            "name": "Bus_11_15min",
            "buffer": 0.25,
            "am_av_hdwy": [
                {"operator": ">", "value": 10},
                {"operator": "<=", "value": 15},
            ],
            "pm_av_hdwy": [
                {"operator": ">", "value": 10},
                {"operator": "<=", "value": 15},
            ],
        }
        filter_criteria_hdwy_15_to_30 = {
            "name": "Bus_15_30min",
            "buffer": 0.25,
            "am_av_hdwy": [
                {"operator": ">", "value": 15},
                {"operator": "<=", "value": 30},
            ],
            "pm_av_hdwy": [
                {"operator": ">", "value": 15},
                {"operator": "<=", "value": 30},
            ],
        }

        filter_criteria_hdwy_gt30 = {
            "name": "Bus_30plusmin",
            "buffer": 0.25,
            "am_av_hdwy": [{"operator": ">", "value": 30}],
            "pm_av_hdwy": [{"operator": ">", "value": 30}],
        }

        # collect filter criteria for selecting headways and what kind of buffer to apply to stops
        criteria_list_cat6 = [
            filter_criteria_major_stop,
            filter_criteria_hdwy_lt10,
            filter_criteria_hdwy_11_to_15,
            filter_criteria_hdwy_15_to_30,
            filter_criteria_hdwy_gt30,
        ]

        # ### Create transit areas with *6-way* categorization

        # using current transit service
        transit_areas_current_cat6, transit_areas_current_dissolved_cat6 = (
            create_multiple_transit_areas(
                transit_current,
                criteria_list_cat6,
                "current_cat6",
                True,
                today_path,
                logger=logger,
            )
        )

        # using current plus fbp transit service
        transit_areas_fbp_cat6, transit_areas_fbp_dissolved_cat6 = (
            create_multiple_transit_areas(
                transit_fbp,
                criteria_list_cat6,
                "fbp_cat6",
                True,
                today_path,
                logger=logger,
            )
        )

        # using current plus np transit service
        transit_areas_np_cat6, transit_areas_np_dissolved_cat6 = (
            create_multiple_transit_areas(
                transit_np,
                criteria_list_cat6,
                "np_cat6",
                True,
                today_path,
                logger=logger,
            )
        )

        # #### cat6 - six way categorization assignment to parcels

        p10_x_transit_area_np_cat6 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_np_dissolved_cat6,
            slug="np",
            variable="cat6",
        )

        p10_x_transit_area_fbp_cat6 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_fbp_dissolved_cat6,
            slug="fbp",
            variable="cat6",
        )

        p10_x_transit_area_current_cat6 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_current_dissolved_cat6,
            slug="current",
            variable="cat6",
        )

    if RUN_2_WAY_BUFFERS:

        ###########################################################################
        # repeat the exercise one last time, but for a coarse 2-way categorization

        # set criteria for selecting headways and what kind of buffer to apply to selected stops
        # Coarse cut

        # filter criteria for selecting headways and what kind of buffer to apply to selected stops
        filter_criteria_hdwy_lt10 = {
            "name": "frequent_transit",
            "buffer": 0.5,
            "am_av_hdwy": [{"operator": "<=", "value": 10}],
            "pm_av_hdwy": [{"operator": "<=", "value": 10}],
        }

        # collect filter criteria for selecting headways and what kind of buffer to apply to stops
        criteria_list_coarse = [filter_criteria_hdwy_lt10]

        # ### create transit areas with *2 way* categorization (major transit service)

        # using current transit service
        transit_areas_current_cat2, transit_areas_current_dissolved_cat2 = (
            create_multiple_transit_areas(
                transit_current,
                criteria_list_coarse,
                "current_cat2",
                True,
                today_path,
                logger=logger,
            )
        )

        # using current plus fbp transit service
        transit_areas_fbp_cat2, transit_areas_fbp_dissolved_cat2 = (
            create_multiple_transit_areas(
                transit_fbp,
                criteria_list_coarse,
                "fbp_cat2",
                True,
                today_path,
                logger=logger,
            )
        )

        # using current plus np transit service
        transit_areas_np_cat2, transit_areas_np_dissolved_cat2 = (
            create_multiple_transit_areas(
                transit_np,
                criteria_list_coarse,
                "np_cat2",
                True,
                today_path,
                logger=logger,
            )
        )

        # #### cat2 frequent transit vs all other areas - categorization assignment to parcels
        p10_x_transit_area_np_cat2 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_np_dissolved_cat2,
            slug="np",
            variable="cat2",
        )
        p10_x_transit_area_fbp_cat2 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_fbp_dissolved_cat2,
            slug="fbp",
            variable="cat2",
        )
        p10_x_transit_area_current_cat2 = assign_transit_service_areas_to_parcels(
            parcels_geo=p10_topofix_pt,
            transit_areas=transit_areas_current_dissolved_cat2,
            slug="current",
            variable="cat2",
        )


    # #################################################################
    # # Crosswalk creation ############################################
    # # Relate parcels to enclosing transit service areas  ############
    # #################################################################

    # ### Assign service areas

    logger.info("Finished preparing parcel-to-transit service area assignment")

    # Classify topo parcels to transit service areas and tracts and epc based on parcel_id
    # relies on transit_scenario_crossswalk containing parcel-to-transit-service-area at different temporal resolutions

    # get a string of the transit service level categories present in the transit_scenario_crosswalk dict
    svc_area_cats = "-".join(
        pd.Series(list(transit_scenario_crosswalk.keys()))
        .str.split("_")
        .apply(lambda x: x[1])
        .unique()
    )

    # assign various classifiers back to parcels (transit service areas, EPCs, HRAs, etc.)
    p10_topofix_pt2 = add_classifications_to_p10_topo(p10_topofix_pt)

    # set the path for the parcel to transit service area crosswalk produced by the first half of the script
    # if it does - we shouldn't need to rerun that part and can proceed to assignment and summaries

    parcel_classes_gpd_path = Path(
        working_dir_path, "outputs", f"p10_topofix_classified_{svc_area_cats}.parquet"
    )

    logger.info(
        f"Write classified parcel dataset to parquet file"
    )
    p10_topofix_pt2.to_parquet(parcel_classes_gpd_path)

    logger.info(
        "Finished assigning other classification variables to parcels. They are ready for further summary and analysis."
    )
    logger.handlers[0].close()
