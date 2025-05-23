import os
import pandas as pd
import geopandas as gpd
import fiona
from pathlib import Path
import fiona
from shapely.validation import explain_validity
import numpy as np
import logging

# Configure logging to display INFO level messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
ANALYSIS_CRS = 2227

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
    "parks",
    "update_2025",
    "outputs"
)

input_path = Path(
    box_dir,
    "Modeling and Surveys",
    "Urban Modeling",
    "Spatial",
    "parks",
    "update_2025",
    "inputs"
)



def load_park_geo_data():
    """
    Load and preprocess park and land use data for creating the H1 Parks metric for PBA50+.

    This function prepares data for subsequent spatial operations in `clip_parks_to_urban_and_epc()`.

    Data Sources:
    - California Protected Areas Database (CPAD) for parks and open spaces:
      https://data.cnra.ca.gov/dataset/california-protected-areas-database
    - Farmland Mapping and Monitoring Program (FMMP) land use data:
      https://gis.conservation.ca.gov/portal/home/item.html?id=5451303ad3d444b6b88f3fe004ccc49f
    - MTC EPC 2022 vintage:
      https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/draft_equity_priority_communities_pba2050plus_acs2022a/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    - MTC EPC 2018 vintage:
      https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/equity_priority_communities_2025_acs2018/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson
    - Bay Area counties:
      https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/region_county_clp/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson

    """
    # 1. CPAD data - parks and open spaces
    parks_file = os.path.join(input_path, 'cpad_2024b_release', 'CPAD_2024b_SuperUnits.shp')
    logging.info(f"Loading parks shapefile from {parks_file}") 
    parks = gpd.read_file(parks_file).to_crs(ANALYSIS_CRS)

    # 2. Farmland Mapping and Monitoring Program (FMMP) land use data
    fmmp_lu_gdb = os.path.join(input_path, "Important_Farmland_2020.gdb")
    logging.info(f"Loading FMMP land use gdb from {fmmp_lu_gdb}")
    layers = fiona.listlayers(fmmp_lu_gdb)
    logging.info(f"Available layers in gdb: {layers}")

    # Compile FMMP layers into a list - Note that SF is not included in FMMP 
    fmmp_lu_layers = ['alameda2020', 'contracosta2020', 'marin2020', 'napa2020', 
                      'sanmateo2020', 'santaclara2020', 'solano2020', 'sonoma2020']
    urban_list = []
    for layer in fmmp_lu_layers:
        logging.info(f"Reading {layer} county layer from FMMP gdb")
        fmmp_lu_gdf = gpd.read_file(fmmp_lu_gdb, layer=layer) 
        # Filter for urban/built-up land (polygon type D)
        urban_gdf = fmmp_lu_gdf[fmmp_lu_gdf['polygon_ty'] == 'D']
        urban_list.append(urban_gdf)
    
    # Combine urban/built-out areas into a single geodataframe
    logging.info(f"Combining urban/built out areas from FMMP into a single geodataframe")
    urban = pd.concat(urban_list, ignore_index=True).to_crs(ANALYSIS_CRS)

    # 3. EPC data for summaries
    logging.info(f"Loading 2018 and 2022 EPC vintages")
    epc18 = gpd.read_file("https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/equity_priority_communities_2025_acs2018/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson").to_crs(ANALYSIS_CRS)
    epc22 = gpd.read_file("https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/draft_equity_priority_communities_pba2050plus_acs2022a/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson").to_crs(ANALYSIS_CRS)

    # Store EPC data in a dictionary
    epc_dict = {
        "epc18": epc18,
        "epc22": epc22
    }

    # Replace "NA" strings with explicit np.nan and filter for EPC tracts
    for name, epc in epc_dict.items():
        logging.info(f"Filtering {name} to EPC tracts only")
        epc.replace("NA", np.nan, inplace=True)
        epc_dict[name] = epc[epc["epc_class"].notnull()]

    # 4. Bay county data for clipping urban parks in the region
    logging.info(f"Loading Bay Area counties shapefile")
    bay_cnty = gpd.read_file("https://services3.arcgis.com/i2dkYWmb4wHvYPda/arcgis/rest/services/region_county_clp/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson").to_crs(ANALYSIS_CRS)
    bay_cnty = bay_cnty.loc[:, ['coname', 'geometry']].rename(columns={'coname': 'county'}).reset_index(drop=True)

    return parks, urban, epc_dict, bay_cnty


def clip_parks_to_urban_and_epc(parks, urban, epc_dict, bay_cnty):
    """""
    Clips park geometries to urban/built-out areas and EPCs, 
    while resolving any geometric inconsistencies.

    Args:
        parks (GeoDataFrame): Geospatial data of parks from sourced from CPAD.
        urban (GeoDataFrame): Geospatial data representing urban/built-out areas sourced from FMMP.
        epc_dict (dict): Dictionary containing EPC tracts for 2022 and 2018 vintages.
        bay_cnty (GeoDataFrame): Geospatial data of Bay Area counties.
    Returns:
        GeoDataFrame: A GeoDataFrame containing parks clipped to urban areas and EPCs, 
                      with area type and county attributes added.
    """

    # The parks data includes small, complex geometries - ensure any self-intersections or small artifacts are resolved
    logging.info(f"Resolving any self-intersections or small artifacts in the parks layer")
    parks.geometry = parks.geometry.buffer(0)

    # Clip parks to urban/buil-out areas
    logging.info(f"Clipping parks to urban/built-out areas only")
    parks_urban = parks.clip(urban)

    # Add SF parks, since FMMP does not cover SF
    sf = bay_cnty[bay_cnty['county'] == 'San Francisco']
    logging.info(f"San Francisco is not included in the FMMP gdb.  Adding San Francisco parks to Bay Area urban parks geodataframe seperately")
    parks_sf_clip = parks.clip(sf)
    parks_urban = pd.concat([parks_urban, parks_sf_clip], axis=0)

    # Clip urban parks to EPCs
    logging.info(f"Clipping urban parks to EPCs")
    parks_epc18 = parks_urban.clip(epc_dict['epc18'])
    parks_epc22 = parks_urban.clip(epc_dict['epc22'])

    # Map area type to parks
    parks_mapping = {
        "All areas": parks_urban,
        "EPC_18": parks_epc18,
        "EPC_22": parks_epc22
    }

    def add_area_type(parks_mapping):
        parks_list = []
        for area_type, parks in parks_mapping.items():
            logging.info(f"Mapping... {parks} to {area_type}")
            parks.loc[:, 'area_type'] = area_type
            parks_list.append(parks[['area_type', 'geometry']])
        return pd.concat(parks_list, axis=0)

    parks_df = add_area_type(parks_mapping)

    # Join the county attribute
    logging.info(f"Spatially join parks to counties so that the parks layer has a county attribute")
    parks_df = parks_df.sjoin(bay_cnty, how='inner', predicate='intersects')

    return parks_df


def summarize_park_acres(parks_df):
    """
    Summarizes urban park acreage by county and area type (all areas and EPCs).
    
    Parameters:
        parks_df (pandas.DataFrame): A GeoDataFrame containing urban parks data.
    Returns:
        pandas.DataFrame: A DataFrame with the following columns:
            - county_name (str): The name of the county.
            - area_type (str): The type of area.
            - existing_park_acres (float): The total park acreage.
            - year (int): Year corresponding to metrics baseline (2023) for RTP 2025.
    """

    # Calculate park square footage - EPSG 2227 uses US survey feet as its unit of measurment
    logging.info(f"Calculating park area in square feet")
    parks_df['area_sq_ft'] = parks_df.geometry.area

    # Convert square feet to acres
    logging.info(f"Converting park area from square feet to acres")
    sq_ft_per_acre = 43560
    parks_df['existing_park_acres'] = parks_df['area_sq_ft'] / sq_ft_per_acre

    # Sum acreas by county and area type
    logging.info(f"Summarizing park areas by County and EPCs")
    park_acre_summary = parks_df.groupby(['county', 'area_type'])['existing_park_acres'].sum().reset_index()

    # Create a year column 
    park_acre_summary["year"] = 2023

    # Rename county column
    park_acre_summary.rename(columns={'county': 'county_name'}, inplace=True)

    return park_acre_summary


def main():
    # Load data
    parks, urban, epc_dict, bay_cnty = load_park_geo_data()

    # Perform spatial operations
    parks_df = clip_parks_to_urban_and_epc(parks, urban, epc_dict, bay_cnty)

    # Summarize park acres
    park_acre_summary = summarize_park_acres(parks_df)

    # Save output
    output_file = output_path / "existing_park_acres_summary.csv"
    park_acre_summary.to_csv(output_file, index=False)
    print(f"Park acre summary saved to {output_file}")


if __name__ == "__main__":
    main()