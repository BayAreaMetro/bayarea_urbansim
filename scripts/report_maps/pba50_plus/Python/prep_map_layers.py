


"""
Prepare supplementary map layers for the PBA50+ Forecasting and Modeling Report.  Layers are written to the 
map_data directory and can be loaded by pba50_plus.aprx for mapping.
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd
import os
import folium
from folium.plugins import MarkerCluster
import fiona
from shapely.geometry import Polygon, MultiPolygon

CRS = 26910
GCS = 4269

# Assign dirs
M_DRIVE = Path("M:/")
BOX_DIR = Path("E:/Box/")

NP_DIR = Path(
    M_DRIVE,
    "urban_modeling",
    "baus", 
    "PBA50Plus",
    "PBA50Plus_NoProject",
    "PBA50Plus_NoProject_v38"
)
MAP_DATA_DIR = Path(
    "..",
    "map_data"
)
TAZ_SUMMARY_FILE = Path(
    NP_DIR,
    "travel_model_summaries",
    "PBA50Plus_NoProject_v38_taz1_summary_2020.csv"
)
TAZ_INTERIM_FILE = Path(
    NP_DIR,
    "core_summaries",
    "PBA50Plus_NoProject_v38_interim_zone_output_2020.csv"
)
TAZ_SHP_FILE = Path(
    BOX_DIR,
    "Modeling and Surveys/Urban Modeling/Spatial/Zones/TAZ1454/zones1454.shp"
)
PIPELINE_H6_H8_FILE = Path(
    M_DRIVE,
    "urban_modeling",
    "baus", 
    "BAUS Inputs",
    "plan_strategies", 
    "H6_H8_development_pipeline_entries_FBP_MAR.csv"
)
TOC_X_PARCEL_FILE = Path(
    BOX_DIR,
    "Modeling and Surveys/Urban Modeling/Bay Area UrbanSim/BASIS/PBA50Plus/urbansim_geodata_ot50pct.gpkg"
)
BAUS_PARCELS_FILE = Path(
    BOX_DIR,
    "Modeling and Surveys/Urban Modeling/Spatial/Parcels/parcel_geoms_fgdb_out.shp"
)
PARCEL_DIR = Path(
    M_DRIVE,
    "urban_modeling",
    "baus",
    "BAUS Inputs",
    "basis_inputs",
    "crosswalks"
)
METRICS_PARCEL_DIR = Path(
    "..", 
    "parcel_data") # This is written from an intermediate state from the metrics scripts


# Module-level cache for BAUS parcels
_cached_baus_parcels_gdf = None

# Module-level cache for Redwood Ferry TRA
_cached_redwood_ferry_tra_gdf = None


# Helper function to ensure map data directory exists
def ensure_map_data_dir():
    """Create MAP_DATA_DIR if it doesn't exist."""
    MAP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Ensured directory exists: {MAP_DATA_DIR.resolve()}")


# Helper function to load and validate BAUS parcels
def load_and_validate_baus_parcels():
    """Load BAUS parcels and ensure valid geometries."""
    baus_parcels_gdf = gpd.read_file(BAUS_PARCELS_FILE).to_crs(CRS)

    invalid = baus_parcels_gdf[~baus_parcels_gdf.is_valid]
    print(f"Invalid geometries: {len(invalid)}")

    baus_parcels_gdf["geometry"] = baus_parcels_gdf["geometry"].buffer(0)

    invalid = baus_parcels_gdf[~baus_parcels_gdf.is_valid]
    print(f"Invalid geometries after buffer: {len(invalid)}")

    baus_parcels_gdf = baus_parcels_gdf[baus_parcels_gdf.is_valid].copy()
    invalid = baus_parcels_gdf[~baus_parcels_gdf.is_valid]
    print(f"Final invalid geometries: {len(invalid)}")
    
    return baus_parcels_gdf


def get_baus_parcels_gdf():
    """Get BAUS parcels, loading only once and caching for reuse.
    
    Returns:
        GeoDataFrame with validated BAUS parcel geometries
    """
    global _cached_baus_parcels_gdf
    
    if _cached_baus_parcels_gdf is None:
        print("Loading and validating BAUS parcels (will be cached for reuse)...")
        _cached_baus_parcels_gdf = load_and_validate_baus_parcels()
        print(f"Loaded {len(_cached_baus_parcels_gdf)} valid parcels (cached)\n")
    else:
        print("Using cached BAUS parcels")
    
    return _cached_baus_parcels_gdf


def get_redwood_ferry_tra_gdf():
    """Get Redwood Ferry TRA, loading only once and caching for reuse.
    
    Returns:
        GeoDataFrame with Redwood Ferry TRA
    """
    global _cached_redwood_ferry_tra_gdf
    
    if _cached_redwood_ferry_tra_gdf is None:
        print("Loading Redwood Ferry TRA (will be cached for reuse)...")
        _cached_redwood_ferry_tra_gdf = create_transit_rich_areas_layer()
        print("Redwood Ferry TRA cached\n")
    else:
        print("Using cached Redwood Ferry TRA")
    
    return _cached_redwood_ferry_tra_gdf


# 1: TAZ-level Maps
def create_taz_level_layer():
    """Create TAZ-level maps with res units, hh, emp, and non-residential sqft per acre."""
    ensure_map_data_dir()
    print("  Loading TAZ summary data...")
    TAZ_SUMM_DF = pd.read_csv(TAZ_SUMMARY_FILE)
    taz_df = TAZ_SUMM_DF.copy()

    print("  Loading TAZ interim data...")
    TAZ_INTERIM_DF = pd.read_csv(TAZ_INTERIM_FILE)
    taz_interim_df = TAZ_INTERIM_DF.copy()

    print("  Merging summary and interim data...")
    taz_df = taz_df.merge(
        taz_interim_df, on="TAZ", how="left"
    )

    print("  Calculating mapping fields...")
    taz_df["pct_sfdu"] = taz_df["SFDU"]/taz_df["RES_UNITS"]
    taz_df["pct_mfdu"] = taz_df["MFDU"]/taz_df["RES_UNITS"]
    taz_df["nonres_sqft_per_acre"] = taz_df["non_residential_sqft"]/taz_df["TOTACRE"]
    taz_df["res_units_per_acre"] = taz_df["RES_UNITS"]/taz_df["TOTACRE"]
    taz_df["hh_per_acre"] = taz_df["TOTHH"]/taz_df["TOTACRE"]
    taz_df["emp_per_acre"] = taz_df["TOTEMP"]/taz_df["TOTACRE"]


    print("  Loading TAZ geometries and reprojecting to CRS...")
    TAZ_GDF =gpd.read_file(TAZ_SHP_FILE).to_crs(CRS)
    taz_gdf = TAZ_GDF.copy()

    print("  Merging data with geometries...")
    taz_gdf = taz_gdf.merge(
        taz_df, left_on="taz1454", right_on = "TAZ", how="left"
    )

    print("  Selecting output columns...")
    taz_gdf = taz_gdf[["taz1454", "pct_sfdu", "pct_mfdu", "nonres_sqft_per_acre", "res_units_per_acre", "hh_per_acre", "emp_per_acre", "geometry"]]

    TAZ_GDF_OUTPUT = Path(
        MAP_DATA_DIR,
        "baus_taz_data.gpkg"
    )
    taz_gdf.to_file(TAZ_GDF_OUTPUT, layer="baus_taz_data", driver="GPKG")
    print(f"TAZ-level maps written to {TAZ_GDF_OUTPUT}")



# 2. Pipeline developments for Mall/Office Park Conversion Projects
def create_mall_office_conversion_layer():
    """Create map layer for mall and office park conversion projects."""
    ensure_map_data_dir()
    print("  Loading adaptive reuse strategy pipeline entries...")
    PIPELINE_H6_H8 = pd.read_csv(PIPELINE_H6_H8_FILE)
    pipeline_h6_h8 = PIPELINE_H6_H8.copy()

    print("  Filtering to mall/office conversions...")
    mall_office_conversions = pipeline_h6_h8[pipeline_h6_h8["source"] == "mall_office"]
    MALL_OFFICE_CONVERSIONS_OUTPUT = Path(
        MAP_DATA_DIR,
        "mall_office_conversions.csv"
    )

    print("  Creating GeoDataFrame from point coordinates...")
    mall_office_conversions_gdf = gpd.GeoDataFrame(
        mall_office_conversions, geometry=gpd.points_from_xy(
            mall_office_conversions.x, mall_office_conversions.y, crs=GCS) # x/y in degrees so use GCS
    )
    MALL_OFFICE_CONVERSIONS_GDF_OUTPUT = Path(
        MAP_DATA_DIR,
        "mall_office_conversions.gpkg"
    )
    print("  Writing to GeoPackage...")
    mall_office_conversions_gdf.to_file(MALL_OFFICE_CONVERSIONS_GDF_OUTPUT, driver="GPKG")
    print(f"Mall/office conversions written to {MALL_OFFICE_CONVERSIONS_GDF_OUTPUT}")



# 3. Publicly owned land development projects
def create_public_land_development_layer():
    """Create map layer for publicly owned land development projects."""
    ensure_map_data_dir()
    print("  Loading adaptive reuse strategy pipeline entries...")
    PIPELINE_H6_H8 = pd.read_csv(PIPELINE_H6_H8_FILE)
    pipeline_h6_h8 = PIPELINE_H6_H8.copy()
    
    print("  Filtering to publicly owned land developments...")
    pub_land_devels = pipeline_h6_h8[pipeline_h6_h8["source"] == "pub"]

    print("  Creating GeoDataFrame from point coordinates...")
    pub_land_devels_gdf = gpd.GeoDataFrame(
        pub_land_devels, geometry=gpd.points_from_xy(
            pub_land_devels.x, pub_land_devels.y, crs=GCS
        )
    )
    PUB_LAND_DEVELS_GDF_OUTPUT = Path(
        MAP_DATA_DIR,
        "pub_land_devels.gpkg"
    )
    print("  Writing to GeoPackage...")
    pub_land_devels_gdf.to_file(PUB_LAND_DEVELS_GDF_OUTPUT, driver="GPKG")
    print(f"Public land developments written to {PUB_LAND_DEVELS_GDF_OUTPUT}")



# 4. Urban Growth Boundaries
def create_urban_growth_boundary_layer(baus_parcels_gdf=None):
    """Create Urban Growth Boundary layers for different scenarios.
    
    Args:
        baus_parcels_gdf: GeoDataFrame with validated parcel geometries.
                         If None, will load from cache or file.
    """    
    ensure_map_data_dir()
    
    if baus_parcels_gdf is None:
        baus_parcels_gdf = get_baus_parcels_gdf()

    PARCEL_FILES = [ # NP and Plan use different UGB defined in seperate parcel geo files
        "parcels_geography_NP_2024_04_29.csv",
        "fbp_urbansim_parcel_classes_ot50pct_feb25_rwc_update_2025.csv"
    ]
    SCENARIO_NAMES = [
        "no_project",
        "final_blueprint",
    ]

   
    ugb_dissolve_dict = {} # To store dissolved geometries for post-processing

    for file, name in zip(PARCEL_FILES, SCENARIO_NAMES):
        print(f"Processing scenario: {name} ({file})")
        print("  Reading parcel CSV...")
        ugb_df = pd.read_csv(PARCEL_DIR / file)
        ugb_df = ugb_df.rename(columns={"PARCEL_ID": "parcel_id"})

        print(f"  UGB value counts before filter for {name}:")
        print(ugb_df["ugb_id"].value_counts(dropna=False))

        
        ugb_df = ugb_df[ugb_df["ugb_id"].notnull()]
        
        print(f"  Filtered to {len(ugb_df)} parcels with values for ugb_id")
        print(f"  UGB value counts after filter for {name}:")
        print(ugb_df["ugb_id"].value_counts(dropna=False))

        print("  Merging with parcel geometries...")
        ugb_gdf = gpd.GeoDataFrame(
            ugb_df.merge(baus_parcels_gdf, on="parcel_id", how="left", validate="1:1"),
            geometry="geometry", crs=CRS
        )
        print(f"{name}, records after joing to geometry: {len(ugb_gdf)}")


        ugb_gdf = ugb_gdf[
            [col for col in ugb_gdf.columns if col.endswith("_id") and col != "pba50_gg_id"] + ["geometry"]
        ]

        print("  Buffering geometries...")
        ugb_buffer = ugb_gdf.buffer(100)
        print("  Dissolving geometries...")
        ugb_dissolve = ugb_buffer.unary_union

        ugb_dissolve = gpd.GeoDataFrame(
            geometry=[ugb_dissolve], crs=CRS
        )
        
        # Store dissolved geometry for later processing
        ugb_dissolve_dict[name] = ugb_dissolve
        
        print(f"Finished processing scenario: {name}\n")

    # Split no_project using final_blueprint - see this comment for why: https://app.asana.com/1/11860278793487/task/1212780032489202/comment/1213035613708028?focus=true
    # Later in ArcGIS Pro, some no project features are stylized manually
    print("Splitting no_project UGB using final_blueprint UGB...")
    no_project_geom = ugb_dissolve_dict["no_project"].geometry.iloc[0]
    final_blueprint_geom = ugb_dissolve_dict["final_blueprint"].geometry.iloc[0]
    
    print("  Performing difference operation...")
    # Get areas in no_project that are not in final_blueprint
    difference_geom = no_project_geom.difference(final_blueprint_geom)
    
    # Convert to individual polygons
    if difference_geom.geom_type == 'MultiPolygon':
        print(f"  Result is MultiPolygon with {len(difference_geom.geoms)} polygons")
        split_gdf = gpd.GeoDataFrame(
            geometry=list(difference_geom.geoms), crs=CRS
        )
    elif difference_geom.geom_type == 'Polygon':
        print(f"  Result is single Polygon")
        split_gdf = gpd.GeoDataFrame(
            geometry=[difference_geom], crs=CRS
        )
    else:
        print(f"  Warning: Unexpected geometry type: {difference_geom.geom_type}")
        split_gdf = gpd.GeoDataFrame(
            geometry=[difference_geom], crs=CRS
        )
    
    # Write split no_project layer
    output_path = MAP_DATA_DIR / "no_project_ugb.gpkg"
    print(f"  Writing split no_project UGB to {output_path}")
    split_gdf.to_file(output_path, layer="no_project_ugb", driver="GPKG")
    print(f"Finished no_project split layer with {len(split_gdf)} features\n")
    
    # Write final_blueprint layer
    output_path = MAP_DATA_DIR / "final_blueprint_ugb.gpkg"
    print(f"  Writing final_blueprint UGB to {output_path}")
    ugb_dissolve_dict["final_blueprint"].to_file(output_path, layer="final_blueprint_ugb", driver="GPKG")
    print(f"Finished final_blueprint layer\n")


def create_transit_rich_areas_layer(baus_parcels_gdf=None):
    """Create Transit Rich Areas (TRA) layer.
    
    Args:
        baus_parcels_gdf: GeoDataFrame with validated parcel geometries.
                         If None, will load from cache or file.
        
    Returns:
        GeoDataFrame with Redwood Ferry TRA for use in TOC function
    """
    ensure_map_data_dir()
    
    if baus_parcels_gdf is None:
        baus_parcels_gdf = get_baus_parcels_gdf()
    
    print("  Loading parcel data...")
    tra_parcels = pd.read_csv(METRICS_PARCEL_DIR / "Final Blueprint_parcel.csv") # I don't recall why this file was written from an intermediate state in metrics script
    tra_parcels = tra_parcels[["parcel_id", "jurisdiction", "tra_id"]][tra_parcels["tra_id"].notnull()]

    print(f"  Filtered to {len(tra_parcels)} parcels with TRA designations")

    print("  Merging with parcel geometries...")
    tra_parcels_gdf = gpd.GeoDataFrame(tra_parcels.merge(
        baus_parcels_gdf, on="parcel_id", how="left"
    ), geometry="geometry", crs=CRS)

    print("  Buffering TRA parcel geometries...")
    tra_parcels_gdf["geometry"] = tra_parcels_gdf.buffer(50)


    print("  Extracting Redwood Ferry TRA for later TOC layer creation...")
    redwood_ferry_tra_gdf = tra_parcels_gdf[
        (tra_parcels_gdf["jurisdiction"] == "Redwood City") &
        (tra_parcels_gdf["tra_id"] == "tra_5")
    ]

    print("  Dissolving TRA parcels by tra_id...")
    tra_parcels_dissolved = tra_parcels_gdf.dissolve(by="tra_id")

    tra_parcels_dissolved = tra_parcels_dissolved.reset_index()


    TRA_GDF_OUTPUT = Path(
        MAP_DATA_DIR,
        "tra_dissolve.gpkg"
    )

    print("  Writing TRA dissolved layer to GeoPackage...")
    tra_parcels_dissolved.to_file(TRA_GDF_OUTPUT, driver="GPKG")
    print(f"Transit rich areas written to {TRA_GDF_OUTPUT}")
    
    print(f"Returning redwood_ferry_tra_gdf for use in create_toc_layer()")
    return redwood_ferry_tra_gdf


# 6. Growth geo within and outside HRAs
def create_hra_layer(baus_parcels_gdf=None):
    """Create High Resource Areas (HRA) layers.
    
    Args:
        baus_parcels_gdf: GeoDataFrame with validated parcel geometries.
                         If None, will load from cache or file.
    """
    ensure_map_data_dir()
    
    if baus_parcels_gdf is None:
        baus_parcels_gdf = get_baus_parcels_gdf()
    
    print("  Loading parcel data and filtering to growth geographies...")
    gg_hra_parcels = pd.read_csv(METRICS_PARCEL_DIR / "Final Blueprint_parcel.csv")
    gg_hra_parcels = gg_hra_parcels[gg_hra_parcels["gg_id"].notnull()]

    print("  Filling null HRA values with 'NonHRA'...")
    gg_hra_parcels["hra_id"] = gg_hra_parcels["hra_id"].fillna("NonHRA")

    print("  Merging with parcel geometries...")
    gg_hra_parcels_gdf = gpd.GeoDataFrame(gg_hra_parcels.merge(
        baus_parcels_gdf, on="parcel_id", how="left"
    ), geometry="geometry", crs=CRS)

    print("  Selecting output columns...")
    gg_hra_parcels_gdf = gg_hra_parcels_gdf[["parcel_id", "gg_id", "hra_id", "geometry"]]

    print("  Buffering HRA parcel geometries...")
    gg_hra_parcels_gdf["geometry"] = gg_hra_parcels_gdf.buffer(50)

    print("  Dissolving parcels by hra_id...")
    gg_hra_dissolved = gg_hra_parcels_gdf.dissolve(by="hra_id")

    gg_hra_dissolved = gg_hra_dissolved.reset_index()


    # HRA_GDF_OUTPUT = Path(
    #     MAP_DATA_DIR,
    #     "hra_dissolve.gpkg"
    # )
    # gg_hra_dissolved.to_file(HRA_GDF_OUTPUT, driver="GPKG")
    print("  Writing HRA dissolved layer to shapefile...")
    HRA_GDF_OUTPUT = Path(
        MAP_DATA_DIR,
        "hra_dissolve.shp"
    )
    gg_hra_dissolved.to_file(HRA_GDF_OUTPUT, driver="ESRI Shapefile")
    print(f"HRA layers written to {HRA_GDF_OUTPUT}")




# 7. TOCs
def create_toc_layer(redwood_ferry_tra_gdf=None):
    """Create Transit-Oriented Communities (TOC) layers.
    
    Args:
        redwood_ferry_tra_gdf: GeoDataFrame with Redwood Ferry TRA to include in TOCs.
                              If None, will load from cache or file.
    """
    ensure_map_data_dir()
    
    if redwood_ferry_tra_gdf is None:
        redwood_ferry_tra_gdf = get_redwood_ferry_tra_gdf()
    
    print("  Loading TOC parcel data and reprojecting CRS...")
    TOC_PARCEL_GDF = gpd.read_file(TOC_X_PARCEL_FILE).to_crs(CRS)
    toc_parcel_gdf = TOC_PARCEL_GDF.copy()

    print("  Filtering to parcels with TOC service tier values...")
    toc_parcel_gdf = toc_parcel_gdf[toc_parcel_gdf["service_ti"].notnull()].reset_index()
    toc_parcel_gdf = toc_parcel_gdf[["parcel_id", "service_ti", "geometry"]]

    print("  Buffering TOC parcel geometries...")
    toc_parcel_gdf["geometry"] = toc_parcel_gdf.buffer(50)
    print("  Dissolving TOC parcels by service tier...")
    toc_parcel_dissolve = toc_parcel_gdf.dissolve(by="service_ti")

    print("  Adding Redwood Ferry TRA as TOC...")
    redwood_ferry_tra_to_toc_gdf = redwood_ferry_tra_gdf.copy()

    redwood_ferry_tra_to_toc_gdf = redwood_ferry_tra_to_toc_gdf[["parcel_id", "geometry"]]

    redwood_ferry_tra_to_toc_gdf["parcel_id"] = redwood_ferry_tra_to_toc_gdf["parcel_id"].astype(float)

    redwood_ferry_tra_to_toc_gdf["service_ti"] = 4.0 # Dummy servive tier - not sure if this is the correct value but anything here will do as we're not symbolizing different tiers
    print("  Concatenating Redwood Ferry TRA with TOC layers...")
    toc_parcel_dissolve = pd.concat([toc_parcel_dissolve, redwood_ferry_tra_to_toc_gdf])

    TOC_DISSOLVE_OUTPUT = Path(
        MAP_DATA_DIR,
        "toc_dissolve.gpkg"
    )
    print("  Writing TOC dissolved layer to GeoPackage...")
    toc_parcel_dissolve.to_file(TOC_DISSOLVE_OUTPUT, driver = "GPKG")
    print(f"TOC layers written to {TOC_DISSOLVE_OUTPUT}")


if __name__ == "__main__":

    print("Starting map data preparation...\n")
    
    print("Creating TAZ-level maps...")
    create_taz_level_layer()
    print()
    
    print("Creating mall/office conversion maps...")
    create_mall_office_conversion_layer()
    print()
    
    print("Creating public land development maps...")
    create_public_land_development_layer()
    print()
    
    print("Creating urban growth boundaries...")
    create_urban_growth_boundary_layer() 
    print()
    
    print("Creating transit rich areas...")
    create_transit_rich_areas_layer()  # Will load and cache Redwood Ferry TRA
    print()
    
    print("Creating HRA layers...")
    create_hra_layer()
    print()
    
    print("Creating TOC layers...")
    create_toc_layer()  # Will reuse cached Redwood Ferry TRA
    print()
    
    print("Map data preparation complete!")


