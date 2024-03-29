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

# DVUTILS_LOCAL_CLONE_PATH = "/Users/aolsen/Documents/GitHub/dvutils"
# sys.path.insert(0, DVUTILS_LOCAL_CLONE_PATH)
# from utils_io import *

user = os.getlogin()

# ## Paths
# ### Practical

home_dir = Path.home()
box_dir = Path(home_dir, "Box")

# set the mounted path for M: drive
# in my case, M:\urban_modeling is mounted to /Volumes/Data/Models/urban_modeling
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
working_dir_path = Path(
    m_drive, "Data", "GIS layers", "JobsHousingTransitProximity", "update_2024"
)

# set the path for the parcel to transit service area crosswalk produced by the first half of the script
# if it does - we shouldn't need to rerun that part and can proceed to assignment and summaries

parcel_classes_gpd_path = Path(working_dir_path, "outputs", "p10_topofix_pt2.parquet")

# boolean for whether the main parcel to service area crosswalk already exists
# crosswalk_exists = parcel_classes_gpd_path.exists()

# Configure logging level
def logger_process(working_dir_path, log_name="proximity2transit.log"):

    LOG_LEVEL = logging.DEBUG

    # Create a unique log file path
    LOG_FILE = Path(working_dir_path, log_name)

    # Create the output directory if it doesn't exist
    output_dir = Path(working_dir_path, "outputs")
    output_dir.mkdir(parents=True, exist_ok=True)  # Create parent directories if needed

    # Configure logger
    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)

    # Shared formatter for consistent log messages
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )

    # Console handler with INFO level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler with DEBUG level
    fh = logging.FileHandler(LOG_FILE, mode="w")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


YMD = datetime.now().strftime("%Y%m%d")
today_path = Path(working_dir_path, "outputs", YMD)
today_path.mkdir(exist_ok=True)
logger = logger_process("proximity2transit_{YMD}.log")


# ### Data paths - AGOL URLs, and local versions until we get utils working
MTC_ONLINE_TRANSIT_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/transitstops_existing_planned_2021/FeatureServer/0"

MTC_ONLINE_BACOUNTY_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/region_county/FeatureServer/0"

MTC_ONLINE_TAZ_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/transportation_analysis_zones_1454/FeatureServer/0"

MTC_ONLINE_TRACT_URL = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/cocs_ACS2014_2018/FeatureServer/0"

# # create arc client object
# arcgis_client = create_arcgis_client()

# # set URL
# SOME_URL = 'https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/DRAFT_TOC_Transit_Stations_Existing_June_2023_/FeatureServer'

# # pull data
# transit_data = pull_geotable_agol(base_url=SOME_URL, client=arcgis_client)

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
# extracted from AGOL around March 20, 2024
county_path = Path(
    m_drive,
    "Data",
    "GIS layers",
    "JobsHousingTransitProximity",
    "update_2024",
    "inputs",
    "proximity",
    "region_county_-6301486166122500334.gpkg",
)
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

KM_TO_MILES = 1.609344

# get the inverse
MILES_TO_KM = 1 / KM_TO_MILES


def miles_to_m(miles):
    """Converts miles to meters.

    Args:
      miles: The distance in miles.

    Returns:
      The distance in meters.
    """
    return 1000 * miles / MILES_TO_KM


def data_filtering(input_data, filter_criteria):
    """Filters a DataFrame of transit stops based on specified criteria.

    Args:
        input_data (pd.DataFrame): The DataFrame containing input data.
        filter_criteria (dict): A dictionary where keys are column names and values are tuples
            containing the comparison operator and the value to compare against.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """

    key_numerics = ["am_av_hdwy", "pm_av_hdwy", "major_stop"]

    logger.info("\tStarting length of the dataframe: {:,}".format(len(input_data)))

    # for filter_criteria in list_filter_criteria:
    filter_components = []
    for key, operator_values in filter_criteria.items():
        filter_sub_components = []

        # skip non-criteria related keys in dict
        if not key in ["name", "buffer"]:
            # print(operator_values)
            for operator_value in operator_values:
                value = operator_value["value"]
                operator = operator_value["operator"]

                # Generate query expression list from components

                # if value is a string  - add quotes - otherwise assume to be numeric
                lst_conditions = [
                    (
                        f'{key}{operator}"{value}"'
                        if isinstance(value, str)
                        else f"{key}{operator}{value}"
                    )
                ]

                # Generate query string from query list
                str_conditions = "".join(lst_conditions)
                filter_sub_components.append(str_conditions)

            # we are treating inner components as AND - so am_headway >10 and am_headway_15
            # but we are treating AM and PM headways as OR - a stop with 12 minutes am and 25 minutes pm
            # will be selected with the shorter headway
            str_partial_query = "({})".format(" & ".join(filter_sub_components))
            filter_components.append(str_partial_query)

    # Concatenate component filters
    str_full_query = " | ".join(filter_components)
    logger.info(f"\tQuery string: {str_full_query}")

    # Create mask of matching records from component filters - to drop
    keep_mask = input_data.eval(str_full_query)

    if len(keep_mask.value_counts()) > 1:
        keep_summary = input_data[keep_mask][key_numerics].median()
        
        #series_string = f"\tRecords Filtered: {'; '.join([f'{key}: {value:,.0f}' for key, value in keep_summary.items()])}"

        #         logger.info(
        #             f'Applying filter: `{str_full_query}`\n',
        #             f'\Selecting {keep_mask.value_counts()[True]} records from dataframe\n',
        #             #f'\tRecords lost:\n\t{input_data[keep_mask][key_numerics].sum()}',
        #             series_string)

        # applying to source data
        input_data = input_data[keep_mask]
        logger.info(f"\tRecords after subsetting: {len(input_data):,.0f}")

    else:
        logger.info("Filters match no records. Keeping all records.")

    # Return resulting df
    return input_data


def get_combination_names(group):
    """Constructs a string representing combinations based on True values in a pandas GroupBy.

    Generates a string by joining column names where the corresponding value in the Series is True.

    This function is designed to be used with the `apply` method of a pandas GroupBy object.

    Args:
    group: A Series (from a pandas GroupBy) containing boolean values.

    Returns:
    A string with '-' joining column names that have True values, or an empty string if none.
    """

    combination_names = []
    for col_name, value in group.items():
        if value.any():
            # if there are any true values - keep the corresponding column name
            combination_names.append(col_name)
    return "-".join(combination_names)


def get_combination_names_hierarchy(group):
    """Determines a hierarchical name for a group in a pandas groupby based on True-valued column names.

    Prioritizes the first column with all True values (out of four boolean transit stop buffer categories)
    excluding 'No_Fixed_Route_Transit'.

    This function is designed to be used with the `apply` method of a pandas GroupBy object.

    Args:
    group: A Series (from a pandas GroupBy) containing boolean values for the four transit stop buffer categories.

    Returns:
    A string representing the hierarchical name, either a feature name with '_only'
    suffix, 'none' if only 'No_Fixed_Route_Transit' is True, or the original group
    if no single feature has all True values.
    """

    # We loop through values in the tuple of booleans for each group - ordering matters. So
    # we stop at the first case of a True value - the
    # first one takes primacy - like major stops buffer
    for col, val in group.items():
        # print(col,val)
        if val.all():
            return f"{col}_only" if col != "No_Fixed_Route_Transit" else "none"

    return group


def station_buffer(source_df, buf_dist):
    """Buffers a GeoDataFrame with a specified distance in miles.

    Args:
    source_df: A GeoDataFrame containing the stations to be buffered.
    buf_dist: The distance in miles for the buffer zone.

    Returns:
    A GeoDataFrame representing the stations with a buffer applied
    and dissolved to remove duplicate geometries.

    Logs:
    The size of the dataframe and the buffer applied.
    """
    logger.info(
        f"\tBuffering a df of {len(source_df)} records with {buf_dist:.02f} miles"
    )
    source_buf = source_df.buffer(miles_to_m(buf_dist))

    output = (
        gpd.GeoDataFrame(  # data=source_subset[passthrough_cols],
            geometry=source_buf.values
        )
        .reset_index()
        .rename(columns={"index": "rowid"})
    )

    return output.dissolve()


def col_values(s, colname):
    msg = f"{colname} records with no value assigned: {len(s[s.isna()])}"
    # logger.info(msg)
    logger.info(msg)


# TODO: should probably not be in global name space
transit_buffers = {}


def create_multiple_transit_areas(
    transit_stop_gdf, criteria_list, slug="current", write_to_disk=False
):
    """
    Creates multiple transit service areas with hierarchical prioritization, based on criteria.

    **Arguments:**

    - `transit_stop_gdf` (GeoDataFrame): The input GeoDataFrame containing transit stop data.
    - `criteria_list` (list): A list of dictionaries, where each dictionary specifies filter criteria for a transit stop subset.
    - `slug` (str, optional): A unique identifier for the current variant (default: "current") (refers to existing transit levels of service.
    - `write_to_disk` (bool, optional): A flag indicating whether to write the output GeoDataFrames to disk (default: False).

    **Input Assumptions:**

    - The `transit_stop_gdf` GeoDataFrame is assumed to have columns with information about transit stops.
    - Each dictionary in the `criteria_list` is expected to have keys like "name" (criteria name) and filter variables like am headway (see `data_filtering` function).
    - External functions `data_filtering` and `station_buffer` are assumed to be defined elsewhere for filtering and buffering transit stops.
    - An external function `get_combination_names_hierarchy` (used for determining highest priority category) and `get_combination_names` (used for listing all transit categories) are assumed to be defined elsewhere.
    - A logger object is assumed to be in the global namespace.

    **Returns:**

    - `prior_frame` (GeoDataFrame): The un-dissolved GeoDataFrame after processing individual transit layers and combining them based on priority.
    - `prior_frame_dissolved` (GeoDataFrame): The dissolved GeoDataFrame representing distinct transit service level zones.
    """

    logger.info(f"***Running create_transit_areas function for variant: {slug}***")
    logger.info(f"Transit Stop input geodataframe has shape: {transit_stop_gdf.shape}")

    # store filtered and buffered data
    buffered_stops = {}

    # store the names of the passed filter criteria - order matters to the hierarchy
    # and needs to be known to the user - that major stops will trump 15 min headways, will trum 30 min headways etc
    source_frames_ordered = []

    logger.info("***Looping through criteria list to subset transit stops***")

    # filter transit data, and in the same go, buffer the stops

    for filter_criteria in criteria_list:
        logger.info(f'Transit Station Subset: {filter_criteria["name"]}')

        # store filter condition name
        source_frames_ordered.append(filter_criteria["name"])

        # filter transit data
        transit_filtered = data_filtering(transit_stop_gdf, filter_criteria)

        # then apply the buffer
        transit_filtered_buffered = station_buffer(
            transit_filtered, filter_criteria["buffer"]
        )

        # store filtered and buffered data in a dict
        buffered_stops[filter_criteria["name"]] = transit_filtered_buffered

        # write out the filtered transit data mainly for testing purposes
        transit_filtered.to_file(
            Path(today_path, "transit_filter_{}.gpkg".format(filter_criteria["name"])),
            driver="GeoJSON",
            engine="pyogrio",
        )

    # Also add the region as a final separate layer to union with, for wall-to-wall coverage
    # TODO: find a more generalized approach for this than topping off with a regional layer
    # of course could just omit the regional layer entirely and fillna on the parcel side
    buffered_stops["No_Fixed_Route_Transit"] = region_bdry

    # store the name as well
    source_frames_ordered.append("No_Fixed_Route_Transit")

    # Prepare unioning of the different buffer categories to identify transit service areas
    logger.info(
        "***Union the transit stop buffers to identify transit service areas***"
    )

    # We grab the first element in the service category list as the
    major_buf_name = source_frames_ordered[0]

    # Initialize a prior frame as a starting point for the union
    prior_frame = buffered_stops[major_buf_name].rename(
        columns={"rowid": major_buf_name}
    )

    i = 1
    for nme, current_frame in buffered_stops.items():

        # don't union with the first one which is itself
        if nme != major_buf_name:
            logger.info(f'\tUnion {i}: {"->".join(list(buffered_stops)[:i])}->{nme}')

            # Then in each loop we get a new prior_frame which is just the union
            # of past unions, and union with the current_frame, building up the unions as we go

            prior_frame = gpd.overlay(
                prior_frame,
                current_frame.rename(columns={"rowid": nme}),
                how="union",
                keep_geom_type=True,
            )

            logger.info(f'\tColumns: {"; ".join(prior_frame.columns)}')

            # this is a topology hack to avoid linestring
            # TopologyException: found non-noded intersection between LINESTRING
            prior_frame["geometry"] = prior_frame["geometry"].buffer(0.01)
            i += 1

    #     logger.info(
    #         f'Result: a unioned frame of {len(prior_frame)} geographies')

    # In this built-up frame, a missing value for a column means that a certain area is outside the
    # current column's transit buffer.
    prior_frame = prior_frame.fillna(-1).replace({0: True, -1: False})

    # get a unique id for the combinations of presence / absence of different buffer types

    prior_frame["combination_id"] = prior_frame.groupby(source_frames_ordered).ngroup()

    # Then, for each combination of overlapping areas, get the most important
    # where rail (major stop) trumps shorter headways, which in turn trumps longer headways etc

    # get a count of how many distinct categories are enumerated
    cat_count = len(source_frames_ordered)
    cat_count_var = f"cat{cat_count}"

    prior_frame[cat_count_var] = prior_frame.combination_id.map(
        prior_frame.groupby(["combination_id"])[source_frames_ordered].apply(
            get_combination_names_hierarchy
        )
    )

    # Get the full string list of components in any given unioned area - might be in the vicinity
    # of a major stop, 15 min headway, 30 min headway, or just one or none of them. Which?

    prior_frame["combination_id_desc"] = prior_frame.combination_id.map(
        prior_frame.groupby(["combination_id"])[source_frames_ordered].apply(
            get_combination_names
        )
    )

    # dissolve to just the core service category areas
    prior_frame_dissolved = prior_frame.dissolve([cat_count_var], as_index=False)

    logger.info(
        f"Result: a unioned frame of {len(prior_frame)} component geographies, dissolved to {len(prior_frame_dissolved)}"
    )

    if write_to_disk:
        logger.info(f"Writing gdf of transit service level areas to disk for {slug}")

        out_path = Path(today_path, f"transit_service_levels_{slug}.geojson")
        prior_frame_dissolved.to_file(out_path, driver="GeoJSON", engine="pyogrio")

        # out_path = Path(today_path,f'transit_service_levels_{slug}.gpkg')
    #         prior_frame_dissolved.to_file(
    #             out_path,
    #             driver='GPKG',engine='pyogrio')

    # lastly, store a copy in this dict in the global namespace 
    transit_buffers[slug] = prior_frame_dissolved
    return prior_frame, prior_frame_dissolved


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

    return mapping


# This could be handled by more general lookups from DataViz.


def add_classifications_to_p10_topo(p10_topofix_pt):

    ####################################################
    # add tractids, tazs to topo parcels

    # vintage 2010
    p10_topofix_pt["tract10"] = p10_topofix_pt.PARCEL_ID.map(
        parcel_x_tracts.set_index("parcel_id").tract10.map(lambda x: f"{x:011.0f}")
    )
    col_values(p10_topofix_pt["tract10"], "tract10")

    # vintage 2020
    p10_topofix_pt["tract20"] = p10_topofix_pt.PARCEL_ID.map(
        parcel_x_tracts.set_index("parcel_id").tract20.map(lambda x: f"{x:011.0f}")
    )
    col_values(p10_topofix_pt["tract20"], "tract20")

    # add TAZs
    p10_topofix_pt["taz"] = p10_topofix_pt.PARCEL_ID.map(
        parcel_x_taz.set_index("PARCEL_ID").ZONE_ID
    )
    col_values(p10_topofix_pt["taz"], "taz")

    ####################################################

    # add area type based on the just assigned taz
    p10_topofix_pt["area_type"] = p10_topofix_pt.taz.map(taz_x_areatype)
    col_values(p10_topofix_pt["area_type"], "area_type")

    # add epc (RTP2021 version) to parcels using tract10
    p10_topofix_pt["epc21"] = p10_topofix_pt.tract10.map(
        tract_epc21_file.set_index("geoid10").epc_class.fillna("Not EPC")
    )
    col_values(p10_topofix_pt["epc21"], "epc21")

    p10_topofix_pt["is_epc21"] = (p10_topofix_pt.epc21 != "Not EPC").map(
        {True: "CoCs", False: "Not CoCs"}
    )
    col_values(p10_topofix_pt["is_epc21"], "is_epc21")

    # add epc (RTP2025 version) to parcels using tract10
    p10_topofix_pt["epc24"] = p10_topofix_pt.tract20.map(
        tract_epc24_file.set_index("geoid20").epc_class.fillna("Not EPC")
    )
    col_values(p10_topofix_pt["epc24"], "epc24")

    p10_topofix_pt["is_epc24"] = (p10_topofix_pt.epc24 != "Not EPC").map(
        {True: "CoCs", False: "Not CoCs"}
    )
    col_values(p10_topofix_pt["is_epc24"], "is_epc24")

    # add hra23 to parcels using tract10 (!)
    p10_topofix_pt["hra23"] = p10_topofix_pt.tract10.map(
        tract_hra_2023.set_index("geoid").hra_category
    )
    col_values(p10_topofix_pt["hra23"], "hra23")

    p10_topofix_pt["is_hra23"] = p10_topofix_pt.hra23.isin(
        ["High Resource", "Highest Resource"]
    ).map({True: "HRAs", False: "Not HRAs"})
    col_values(p10_topofix_pt["is_hra23"], "is_hra23")

    # add hra19 to parcels using tract10
    p10_topofix_pt["hra19"] = p10_topofix_pt.tract10.map(
        tract_hra_2019.set_index("geoid").hra_category
    )
    col_values(p10_topofix_pt["hra19"], "hra19")

    p10_topofix_pt["is_hra19"] = p10_topofix_pt.hra19.isin(
        ["High Resource", "Highest Resource"]
    ).map({True: "HRAs", False: "Not HRAs"})
    col_values(p10_topofix_pt["is_hra19"], "is_hra19")

    # assign different service level resolution classifications to parcels
    for key, val in transit_scenario_crosswalk.items():
        service_level_var = f"Service_Level_{key}"

        p10_topofix_pt[service_level_var] = p10_topofix_pt.PARCEL_ID.map(
            transit_scenario_crosswalk[key]
        )
        col_values(p10_topofix_pt[service_level_var], service_level_var)

    return p10_topofix_pt


# ### Load data
logger.info("Begin loading data")

logger.info("\tLoad parcel geoms")
p10_topofix = gpd.read_feather(parcel_topo_file)
p10_topofix["geom_pt"] = p10_topofix.representative_point()
p10_topofix_pt = p10_topofix.set_geometry("geom_pt")[["PARCEL_ID", "geom_pt"]]

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

# Load county gdf
logger.info("\tLoading county geoms")
county_file = gpd.read_file(county_path)
county_file = county_file.to_crs(ANALYSIS_CRS)

# load transit stops gdf
logger.info("\tLoading transit stops")
transit_file = gpd.read_file(transit_path)
transit_file = transit_file.to_crs(ANALYSIS_CRS)

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
tract_epc24_file["geoid20"] = tract_epc24_file.tract_geoid.map(lambda x: f"{x:011d}")

# heuristic confirmation of tract vintages for EPC files based on overlap with tract geodata

# def confirm_tract_vintage(fl, tr, fl_col, tr_col):
#     output = list(set(fl[fl_col])-set(tr[tr_col]))
#     print('Unmatched tracts: ', len(output))
#     return output

# # check that epc21 tracts are well matched in census 2010 vintage
# # ok - just 5 tracts - mostly water - not in the tract file
# confirm_tract_vintage(fl=tract_epc21_file, tr=tract_2010_file,
#                       fl_col='geoid10', tr_col='GEOID10')

# # check that epc24 tracts are well matched in census 2020 vintage
# # ok - just 6 tracts - mostly water - not in the tract file
# confirm_tract_vintage(fl=tract_epc24_file, tr=tract_2020_file,fl_col='geoid20',tr_col='GEOID')

# confirm_tract_vintage(fl=tract_hra_2019, tr=tract_2010_file,fl_col='geoid',tr_col='GEOID10')

# # HRA 2023 works nicely with tracts 2010 - just 5 unmatched tracts

# confirm_tract_vintage(fl=tract_hra_2023, tr=tract_2010_file,fl_col='geoid',tr_col='GEOID10')

# HRA 2023 does not work nicely with tracts 2020:
# HRA 2023 version has lots of unmatched tracts against the tract 2020 file

# confirm_tract_vintage(fl=tract_hra_2023, tr=tract_2020_file,fl_col='geoid',tr_col='GEOID')

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

# load a feather version of p10 parcels after the topology fixes
# https://app.asana.com/0/1202179257399094/1205973096797373/f


# col_values(parcel_x_tracts.tract10, 'trt')

# # Create buffer for transit layer

# ## Step 0: Subset transit data to relevant universes

# we need a regional wall-to-wall boundary for the transit buffer union "everything else" areas

region_bdry = gpd.GeoDataFrame(
    data={"No_Fixed_Route_Transit": [0]}, geometry=county_file.dissolve()["geometry"]
)
region_bdry.index = region_bdry.index.set_names("rowid")

# we use status criteria OR major_stop here -
# following logic here: https://github.com/BayAreaMetro/petrale/blob/c4b96a98b291ada58e065375beccd5bf11e2da1b/scripts/proximity2transit.py#L128

# subset to existing, under construction, open or FBP

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


if __name__ == "__main__":

    # ## Step 1a: define filters for buffers and headways on transit stops - each category is defined separately, and we combine in a list

    # set criteria for selecting headways and what kind of buffer to apply to selected stops

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
        "am_av_hdwy": [{"operator": ">", "value": 15}, {"operator": "<=", "value": 30}],
        "pm_av_hdwy": [{"operator": ">", "value": 15}, {"operator": "<=", "value": 30}],
    }

    filter_criteria_hdwy_gt30 = {
        "name": "Bus_30plusmin",
        "buffer": 0.25,
        "am_av_hdwy": [{"operator": ">", "value": 30}],
        "pm_av_hdwy": [{"operator": ">", "value": 30}],
    }

    criteria_list = [
        filter_criteria_major_stop,
        filter_criteria_hdwy_lt15,
        filter_criteria_hdwy_15_to_30,
        filter_criteria_hdwy_gt30,
    ]

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
        "am_av_hdwy": [{"operator": ">", "value": 10}, {"operator": "<=", "value": 15}],
        "pm_av_hdwy": [{"operator": ">", "value": 10}, {"operator": "<=", "value": 15}],
    }
    filter_criteria_hdwy_15_to_30 = {
        "name": "Bus_15_30min",
        "buffer": 0.25,
        "am_av_hdwy": [{"operator": ">", "value": 15}, {"operator": "<=", "value": 30}],
        "pm_av_hdwy": [{"operator": ">", "value": 15}, {"operator": "<=", "value": 30}],
    }

    filter_criteria_hdwy_gt30 = {
        "name": "Bus_30plusmin",
        "buffer": 0.25,
        "am_av_hdwy": [{"operator": ">", "value": 30}],
        "pm_av_hdwy": [{"operator": ">", "value": 30}],
    }

    criteria_list_detail = [
        filter_criteria_major_stop,
        filter_criteria_hdwy_lt10,
        filter_criteria_hdwy_11_to_15,
        filter_criteria_hdwy_15_to_30,
        filter_criteria_hdwy_gt30,
    ]

    # set criteria for selecting headways and what kind of buffer to apply to selected stops
    # Coarse cut

    # filter criteria for selecting headways and what kind of buffer to apply to selected stops
    filter_criteria_hdwy_lt10 = {
        "name": "FREQUENT TRANSIT",
        "buffer": 0.5,
        "am_av_hdwy": [{"operator": "<=", "value": 10}],
        "pm_av_hdwy": [{"operator": "<=", "value": 10}],
    }

    criteria_list_coarse = [filter_criteria_hdwy_lt10]

    # # Chunk A - Run Independent - Parcel Labeling: Form buffers and assign to parcels

    # ## Step A1: create transit buffers
    #
    # ### create transit areas with 5-way categorization

    # using current transit service
    transit_areas_current_cat5, transit_areas_current_dissolved_cat5 = (
        create_multiple_transit_areas(
            transit_current, criteria_list, "current_cat5", True
        )
    )

    # using current plus fbp transit service
    transit_areas_fbp_cat5, transit_areas_fbp_dissolved_cat5 = (
        create_multiple_transit_areas(transit_fbp, criteria_list, "fbp_cat5", True)
    )

    # using current plus np transit service
    transit_areas_np_cat5, transit_areas_np_dissolved_cat5 = (
        create_multiple_transit_areas(transit_np, criteria_list, "np_cat5", True)
    )

    # ### create transit areas with 6-way categorization

    # using current transit service
    transit_areas_current_cat6, transit_areas_current_dissolved_cat6 = (
        create_multiple_transit_areas(
            transit_current, criteria_list_detail, "current_cat6", True
        )
    )

    # using current plus fbp transit service
    transit_areas_fbp_cat6, transit_areas_fbp_dissolved_cat6 = (
        create_multiple_transit_areas(
            transit_fbp, criteria_list_detail, "fbp_cat6", True
        )
    )

    # using current plus np transit service
    transit_areas_np_cat6, transit_areas_np_dissolved_cat6 = (
        create_multiple_transit_areas(transit_np, criteria_list_detail, "np_cat6", True)
    )

    # ### create transit areas with 2 way categorization (major transit service)

    # using current transit service
    transit_areas_current_cat2, transit_areas_current_dissolved_cat2 = (
        create_multiple_transit_areas(
            transit_current, criteria_list_coarse, "current_cat2", True
        )
    )

    # using current plus fbp transit service
    transit_areas_fbp_cat2, transit_areas_fbp_dissolved_cat2 = (
        create_multiple_transit_areas(
            transit_fbp, criteria_list_coarse, "fbp_cat2", True
        )
    )

    # using current plus np transit service
    transit_areas_np_cat2, transit_areas_np_dissolved_cat2 = (
        create_multiple_transit_areas(transit_np, criteria_list_coarse, "np_cat2", True)
    )

    # ## Step A2 - take the just created transit service areas to parcels (before adding any run specific data)

    # ### Assign service areas
    # #### cat5 - six way categorization assignment to parcels

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
    logger.info("Finished preparing parcel-to-transit service area assignment")

    # ### Collect parcel to transit service area mappings / crosswalks in dict

    # store the parcels-to-transit service level correspondences in a dict for easy retrieval

    transit_scenario_crosswalk = {
        # 5-way categorizations
        "cur_cat5": p10_x_transit_area_current_cat5,
        "fbp_cat5": p10_x_transit_area_fbp_cat5,
        "np_cat5": p10_x_transit_area_np_cat5,
        # 6-way categorizations
        "cur_cat6": p10_x_transit_area_current_cat6,
        "fbp_cat6": p10_x_transit_area_fbp_cat6,
        "np_cat6": p10_x_transit_area_np_cat6,
        # binary categorizations
        "cur_cat2": p10_x_transit_area_current_cat2,
        "fbp_cat2": p10_x_transit_area_fbp_cat2,
        "np_cat2": p10_x_transit_area_np_cat2,
    }

    # ## Chunk A Preliminary Result - a parcel file classified with relevant geographies and transit service areas

    # if not crosswalk_exists:

    #     # if the parquet file doesn't exist already, write one
    #     p10_topofix_pt2.to_parquet(parcel_classes_gpd_path)

    # ## Step A3 Classify topo parcels to transit service areas and tracts and epc based on parcel_id
    # relies on transit_scenario_crossswalk above
    p10_topofix_pt2 = add_classifications_to_p10_topo(p10_topofix_pt)
    p10_topofix_pt2.to_parquet(parcel_classes_gpd_path)

    logger.info(
        "Finished assigning other classification variables to parcels. They are ready for further summary and analysis."
    )
    logger.handlers[0].close()
