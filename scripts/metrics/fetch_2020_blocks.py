"""
One-time script to fetch 2020 Census block geometries for the Bay Area via pygris
and save to disk. Run this before metrics_cloud_bespoke.py.
Output: E:\Box\Modeling and Surveys\Census\2020\geo\bay_area_blocks_2020.gpkg
"""
import pygris
from pygris.utils import erase_water
import geopandas as gpd
import pandas as pd
from pathlib import Path

BAY_AREA_COUNTY_FIPS = {
    "Alameda": "001", "Contra Costa": "013", "Marin": "041",
    "Napa": "055", "San Francisco": "075", "San Mateo": "081",
    "Santa Clara": "085", "Solano": "095", "Sonoma": "097",
}

OUTPUT_FILE = Path(r"E:\Box\Modeling and Surveys\Census\2020\geo\bay_area_blocks_2020.gpkg")


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    block_frames = []
    for county, fips in BAY_AREA_COUNTY_FIPS.items():
        print(f"Fetching 2020 blocks for {county}...")
        gdf = pygris.blocks(state="CA", county=fips, year=2020)
        block_frames.append(gdf)

    blocks_gdf = pd.concat(block_frames, ignore_index=True)
    blocks_gdf = blocks_gdf[["GEOID20", "geometry"]]
    print("Erasing water areas...")
    blocks_gdf = erase_water(blocks_gdf)
    blocks_gdf.to_file(OUTPUT_FILE, driver="GPKG")
    print(f"Saved {len(blocks_gdf):,} blocks → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

