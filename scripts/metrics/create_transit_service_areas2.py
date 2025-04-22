import os
import logging
from pathlib import Path
from transit_area_buffer_utils import station_buffer, logger_process
import geopandas as gpd
import pandas as pd

# Constants
ANALYSIS_CRS = "EPSG:26910"

# Paths
user = os.getlogin()
home_dir = Path.home()
box_dir = Path("E:/Box") if user.lower() in ['lzorn', 'jahrenholtz'] else home_dir / 'Box'
m_drive = "M:/" if os.name == "nt" else "/Volumes/Data/Models"

output_path = Path(
    box_dir,
    "Modeling and Surveys",
    "Urban Modeling",
    "Spatial",
    "transit",
    "transit_service_levels",
    "update_2025",
    "outputs"
)

high_quality_transit_stops_input_path = Path(
    m_drive,
    "Data",
    "GIS layers",
    "JobsHousingTransitProximity",
    "update_2025",
    "inputs",
    "proximity"
)


fbp_stops_file = os.path.join(high_quality_transit_stops_input_path, "FBP_HighQualityTransit_Metric.shp")
dbp_stops_file = os.path.join(high_quality_transit_stops_input_path, "DBP_HighQualityTransit_Metric.shp")


parcel_geo_file = Path(
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

# Function to tag parcels within high quality transit stop buffers
def tag_parcels_within_transit_stop_buffer(parcels_geo, stops_file, buffer_distance, logger, column_name):
    logger.info(f"Loading high quality transit stops from {stops_file}")
    stops = gpd.read_file(stops_file).to_crs(ANALYSIS_CRS)
    stop_buffer = station_buffer(stops, buf_dist=buffer_distance, logger=logger)
    
    logger.info("Tagging parcels within high quality transit buffer")
    parcels_within_buffer = gpd.sjoin(parcels_geo, stop_buffer, predicate="within", how="left")
    parcels_within_buffer[column_name] = parcels_within_buffer.index_right.notnull().astype(int)
    return parcels_within_buffer[["PARCEL_ID", column_name]]

# Main function
def main():
    logger = logger_process(output_path, f"parcels10_x_high_quality_stop_buffer.log")

    # Load parcels data
    logger.info("Loading parcels data")
    parcels_geo = gpd.read_feather(parcel_geo_file).to_crs(ANALYSIS_CRS)

    # Process DBP and FBP stops
    dbp_parcels = tag_parcels_within_transit_stop_buffer(parcels_geo, dbp_stops_file, 0.5, logger, "dbp")
    fbp_parcels = tag_parcels_within_transit_stop_buffer(parcels_geo, fbp_stops_file, 0.5, logger, "fbp")

    # Merge results
    logger.info("Merging tagged DBP and FBP tagged parcels into a single dataframe")
    parcels_x_all_stop_buffer = dbp_parcels.merge(fbp_parcels, on="PARCEL_ID", how="outer")

    # Add NP column (identical to DBP)
    parcels_x_all_stop_buffer["np"] = parcels_x_all_stop_buffer["dbp"]

    # Fill missing values with 0 and rename columns
    parcels_x_all_stop_buffer = parcels_x_all_stop_buffer.fillna(0).astype({"dbp": int, "fbp": int, "np": int})
    parcels_x_all_stop_buffer.rename(columns={"PARCEL_ID": "parcel_id"}, inplace=True)

    # Reorder columns
    parcels_x_all_stop_buffer = parcels_x_all_stop_buffer[["parcel_id", "np", "dbp", "fbp"]]

    # Save to CSV
    output_file = output_path / "parcels10_x_high_quality_stop_buffer.csv"
    parcels_x_all_stop_buffer.to_csv(output_file, index=False)

    logger.info(f"Output saved to {output_file}")


if __name__ == "__main__":
    main()