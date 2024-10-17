#!/usr/bin/env python
# coding: utf-8

import os
import logging
import yaml
from datetime import datetime
from pathlib import Path
import pandas as pd
import geopandas as gpd

user = os.getlogin()

# ## Paths
# ### Practical

home_dir = Path.home()
box_dir = Path(home_dir, "Box")

# set the mounted path for M: drive
# in my case, M:\urban_modeling is mounted to /Volumes/Data/Models/urban_modeling
m_drive = "/Volumes/Data/Models" if os.name != "nt" else "M:/"

working_dir_path = Path(
    box_dir,"Modeling and Surveys",
    "Urban Modeling",
    "Spatial",
    "transit",
    "transit_service_levels",
    "update_2024"
)
ANALYSIS_CRS = "EPSG:26910"

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
county_file = gpd.read_file(county_path)
county_file = county_file.to_crs(ANALYSIS_CRS)

# create regional boundary for use in capturing areas outside buffers
region_bdry = gpd.GeoDataFrame(
    data={"No_Fixed_Route_Transit": [0]}, geometry=county_file.dissolve()["geometry"]
)

region_bdry.index = region_bdry.index.set_names("rowid")



KM_TO_MILES = 1.609344

# get the inverse
MILES_TO_KM = 1 / KM_TO_MILES

# add a quick convenience function for miles to m to "talk" to our projected CRS
def miles_to_m(miles):
    """Converts miles to meters.

    Args:
      miles: The distance in miles.

    Returns:
      The distance in meters.
    """
    return 1000 * miles / MILES_TO_KM


def logger_process(
        dir_path:Path,   
        log_name:str = "proximity2transit.log"):

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


def data_filtering(
        input_data:pd.DataFrame,
        filter_criteria:dict,
        logger:logging.Logger
        ) -> pd.DataFrame:
    """Filters a DataFrame of transit stops based on specified criteria.

    Args:
        input_data (pd.DataFrame): The DataFrame containing input data.
        filter_criteria (dict): A dictionary where keys are column names and values are tuples
            containing the comparison operator and the value to compare against.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """

    logger.info("\tStarting length of the dataframe: {}".format(len(input_data)))

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

    # Concatenate component filters - do notice the OR logic assumed here
    str_full_query = " | ".join(filter_components)
    logger.info(f"\tQuery string: {str_full_query}")

    # Create mask of matching records from component filters - to drop
    keep_mask = input_data.eval(str_full_query)

    if len(keep_mask.value_counts()) > 1:
        #keep_summary = input_data[keep_mask][key_numerics].median()

        # series_string = f"\tRecords Filtered: {'; '.join([f'{key}: {value:,.0f}' for key, value in keep_summary.items()])}"

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

def get_combination_names(group,first=True):
    """Constructs a description of the transit service area based on the true values of buffer flags in a group.

    If first=True, it grabs the first, highest order buffer "membership" found and that is the classification.
    Otherwise, a concantenated string is provided for any/all overlapping transit headway buffers.

    Data expectations:
    - This is grouped on a column which by definition has one row - so a tuple of booleans.
    Example row in scope:
    
    Major_Transit_Stop  Bus_<10min  Bus_11_15min  Bus_15_30min  Bus_30plusmin  No_Fixed_Route_Transit
    3       True        True        False         True          True           True
    
     - Note some columns have false; others have true. 
     - When first=True, we grab the first true-valued column from the left.
     - When first=False, we grab all true-valued columns, in order.

    Args:
    group: A Series (from a pandas GroupBy) containing boolean values.
    first: A boolean indicating whether to return the first buffer classification found.
    
    Returns:
    A string describing the transit service area
    """

    if first:
        for col_name, value in group.items():
            # these are technically shape (1,) series - so we need to use any() even if there is just one element in the array to check for truthiness
            
            if value.any():
                out = f"{col_name}_only" if col_name != "No_Fixed_Route_Transit" else "none"
                return out

        # in an edge case of a misplaced parcel, a row could have only false values 
        # - not even inluded in the residual areas of the region. Code as 'none'
        return 'none'
    else:
        combination_names = []
        for col_name, value in group.items():
            if value.any():
                # if there are any true values - keep the corresponding column name
                combination_names.append(col_name)
        out = "-".join(combination_names)
        return out



def station_buffer(source_df, buf_dist, logger):
    """Buffers a GeoDataFrame with a specified distance in miles.

    Args:
    source_df: A GeoDataFrame containing the stations to be buffered.
    buf_dist: The distance in miles for the buffer zone.
    logger: A logging.Logger object to log messages.

    Returns:
    A GeoDataFrame representing the stations with a buffer applied
    and dissolved to remove duplicate geometries.

    Logs:
    The size of the dataframe and the buffer applied.
    """

    if logger is None:
        dir_path = Path.cwd()
        logger = logger_process(
            dir_path=working_dir_path, 
            log_name="transit_buffers.log"
        )

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


def col_values(s,
               colname,
               logger: logging.Logger = None
               ):
    msg = f"{colname} records with no value assigned: {len(s[s.isna()])}"
    # logger.info(msg)
    logger.info(msg)


def create_multiple_transit_areas(
    transit_stop_gdf: gpd.GeoDataFrame,
    criteria_list: list,
    slug: str = "current",
    write_to_disk: bool = False,
    output_folder: Path = None,
    logger: logging.Logger = None,
):
    """Creates multiple transit buffer zones based on specified criteria.

    Args:
        transit_stop_gdf (gpd.GeoDataFrame): A GeoDataFrame containing transit stops.
        criteria_list (list): A list of dictionaries, where each dictionary contains
            filter criteria for a subset of transit stops.
        slug (str, optional): A string to identify the variant of the transit buffer.
            Defaults to "current".
        write_to_disk (bool, optional): A boolean indicating whether to write the
            filtered and buffered transit stops to disk. Defaults to False.
        output_folder (Path, optional): A Path object representing the output folder.
            Defaults to None.
        logger (logging.Logger, optional): A logging.Logger object to log messages.
            Defaults to None.

    Returns:
        tuple: A tuple containing gpd.GeoDataFrames for filtered and buffered transit stops
    """

    if output_folder is None:
        output_folder = Path.cwd() / "output"
        output_folder.mkdir(parents=True, exist_ok=True)

    if logger is None:
        dir_path = Path.cwd()
        logger = logger_process(
            dir_path=dir_path, log_name="transit_service_areas.log"
        )

    logger.info(f"***Running create_transit_areas function for variant: {slug}***")
    logger.info(f"Transit Stop input geodataframe has shape: {transit_stop_gdf.shape}")

    YMD = datetime.now().strftime("%Y%m%d")

    # today_path = Path(output_folder, "transit_buffers",YMD)
    # today_path.mkdir(parents=True, exist_ok=True)

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
        transit_filtered = data_filtering(transit_stop_gdf, filter_criteria, logger)

        # then apply the buffer
        transit_filtered_buffered = station_buffer(
            transit_filtered, filter_criteria["buffer"], logger
        )

        # store filtered and buffered data in a dict
        buffered_stops[filter_criteria["name"]] = transit_filtered_buffered

        # write out the filtered transit data mainly for testing purposes
        if write_to_disk:
            transit_filtered.to_file(
                Path(
                    output_folder,
                    "transit_filter_{}.gpkg".format(filter_criteria["name"]),
                ),
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

    # get a unique id for the combinations of presence / absence of different service buffers

    prior_frame["combination_id"] = prior_frame.groupby(source_frames_ordered).ngroup()

    # Then, for each combination of overlapping areas, get the most important
    # where rail and / or (major stop) trumps shorter headways, which in 
    # turn trumps longer headways etc

    # get a count of how many distinct categories are enumerated
    # if there are 5 categories, we call the var `cat5` etc so 
    # we can distinguish different categorizations (cat2, cat6)
    cat_count = len(source_frames_ordered)
    cat_count_var = f"cat{cat_count}"

    # gets the highest order category
    prior_frame[cat_count_var] = prior_frame.combination_id.map(
        prior_frame.groupby(["combination_id"])[source_frames_ordered].apply(
            get_combination_names,first=True
            #get_combination_names_hierarchy

        )
    )
    logger.info(f"transit service categories: {prior_frame[cat_count_var].value_counts()}" )
    # Get the full string list of components in any given unioned area - might be in the vicinity
    # of a major stop, 15 min headway, 30 min headway, or just one or none of them. Which?

    prior_frame["combination_id_desc"] = prior_frame.combination_id.map(
        prior_frame.groupby(["combination_id"])[source_frames_ordered].apply(
            get_combination_names,first=False
            #get_combination_names
        )
    )
    logger.info(f"transit service categories desc: {prior_frame['combination_id_desc'].value_counts()}")
    
    # dissolve to just the core service category areas
    prior_frame_dissolved = prior_frame.dissolve([cat_count_var], as_index=False)

    logger.info(
        f"Result: a unioned frame of {len(prior_frame)} component geographies, dissolved to {len(prior_frame_dissolved)}"
    )
    # write out the dissolved transit data mainly for testing purposes
    if write_to_disk:
        logger.info(f"Writing gdf of transit service level areas to disk for {slug}")
        out_path = Path(output_folder, f"transit_service_levels_{slug}.geojson")
        prior_frame_dissolved.to_file(out_path, driver="GeoJSON", engine="pyogrio")

        # out_path = Path(today_path,f'transit_service_levels_{slug}.gpkg')
    #         prior_frame_dissolved.to_file(
    #             out_path,
    #             driver='GPKG',engine='pyogrio')

    # lastly, store a copy in this dict in the global namespace
    # transit_buffers[slug] = prior_frame_dissolved
    return prior_frame, prior_frame_dissolved
