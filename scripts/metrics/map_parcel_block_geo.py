"""
map_parcel_block_geo.py

Interactive folium map for QA of parcel-to-block geography classification.
Parcels are colored by their relationship to a requested geography (TRA, GG, PPA, or TOC)
and the area share of that geography within their primary Census block.

Usage:
    from map_parcel_block_geo import list_superdistricts, create_parcel_block_map

    list_superdistricts()
    create_parcel_block_map(["San Francisco"], geo="TRA")
    create_parcel_block_map(["Oakland", "Berkeley"], geo="GG", output_path="map_gg.html")

Prerequisites:
    parcel_block20_xwalk.csv must already exist in COMBO_OUT (built by metrics_cloud_bespoke.py).
"""

import sys
import argparse
from pathlib import Path
from functools import lru_cache

import numpy as np
import pandas as pd
import geopandas as gpd
import folium

sys.path.insert(0, str(Path(__file__).parent))
import metrics_utils

# ---------------------------------------------------------------------------
# Paths (mirror metrics_cloud_bespoke.py)
# ---------------------------------------------------------------------------
METRICS_DIR  = metrics_utils.BOX_DIR / "Plan Bay Area 2050+/Performance and Equity/Plan Performance/Equity_Performance_Metrics/Final_Blueprint"
COMBO_OUT    = Path(r"M:\urban_modeling\urbansim_cloud\projects\combo")
PARCELS_FILE = metrics_utils.BOX_DIR / "Modeling and Surveys/Urban Modeling/Spatial/Parcels/parcel_geoms_fgdb_out.shp"
BLOCKS_2020_FILE = Path(r"E:\Box\Modeling and Surveys\Census\2020\geo\bay_area_blocks_2020.gpkg")
ANALYSIS_CRS = 26910
XWALK_CACHE  = COMBO_OUT / "parcel_block20_xwalk.csv"

# ---------------------------------------------------------------------------
# Classification thresholds and display config
# ---------------------------------------------------------------------------
HALF_AREA = 0.50   # block geo share >= this → "in geo block"

# Class colors built dynamically per geo argument.
# Only in-geo parcels are shown; two classes based on whether the block clears the half-area threshold.
BLOCK_BOUNDARY_COLOR = "#00C853"  # bright green

GEO_CONFIG = {
    "TRA": {"is_col": "is_tra", "share_col": "tra_area_share"},
    "GG":  {"is_col": "is_gg",  "share_col": "gg_area_share"},
    "PPA": {"is_col": "is_ppa", "share_col": "ppa_area_share"},
    "TOC": {"is_col": "is_toc", "share_col": "toc_area_share"},
}

# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_taz_xwalk():
    """Load TAZ crosswalk; initialises metrics_utils module globals as a side-effect."""
    return metrics_utils.load_taz_crosswalks(METRICS_DIR)


@lru_cache(maxsize=1)
def _load_sd_names():
    """Return DataFrame mapping superdistrict int → sd_name string."""
    crosswalks_dir = metrics_utils.M_DRIVE / "urban_modeling" / "baus" / "BAUS Inputs" / "basis_inputs" / "crosswalks"
    sd = pd.read_csv(
        crosswalks_dir / "superdistrict_names.csv",
        encoding="utf-16", sep="\t",
    )[["Superdistrict", "SD Name Old"]]
    sd.rename(columns={"Superdistrict": "superdistrict", "SD Name Old": "sd_name"}, inplace=True)
    return sd


@lru_cache(maxsize=1)
def _load_parcel_geo_data():
    """Return (parcel_block_xwalk, parcel_flags, block_geo) DataFrames.

    parcel_block_xwalk : parcel_id, block_id, intersection_area_sqm, parcel_block_share
    parcel_flags       : parcel_id, block_id, parcel_block_share, is_tra, is_gg, is_ppa, is_toc
    block_geo          : block_id, tra_area_share, gg_area_share, ppa_area_share, toc_area_share
    """
    if not XWALK_CACHE.exists():
        raise FileNotFoundError(
            f"parcel_block20_xwalk.csv not found at {XWALK_CACHE}.\n"
            "Run build_parcel_block20_xwalk() from metrics_cloud_bespoke.py first."
        )

    print("  Loading parcel→block crosswalk...")
    parcel_block_xwalk = pd.read_csv(XWALK_CACHE, dtype={"block_id": str})

    _load_taz_xwalk()  # ensure module globals are populated

    geo_xwalk = (
        metrics_utils.rtp2025_geography_crosswalk_df[["PARCEL_ID", "tra_id", "gg_id", "ppa_id"]]
        .rename(columns={"PARCEL_ID": "parcel_id"})
        .copy()
    )
    toc_xwalk = metrics_utils.rtp2025_parcel_toc_crosswalk_df[["parcel_id", "toc_id"]].copy()

    parcel_flags = (
        parcel_block_xwalk
        .merge(geo_xwalk, on="parcel_id", how="left")
        .merge(toc_xwalk, on="parcel_id", how="left")
    )
    parcel_flags["is_tra"] = parcel_flags["tra_id"].isin({"tra_1","tra_2","tra_3","tra_4","tra_5"})
    parcel_flags["is_gg"]  = parcel_flags["gg_id"]  == "GG"
    parcel_flags["is_ppa"] = parcel_flags["ppa_id"] == "PPA"
    parcel_flags["is_toc"] = parcel_flags["toc_id"] == "toc"

    for geo in ["tra", "gg", "ppa", "toc"]:
        parcel_flags[f"{geo}_area"] = (
            parcel_flags[f"is_{geo}"].astype(float) * parcel_flags["intersection_area_sqm"]
        )

    print("  Computing block-level geo area shares...")
    block_geo = (
        parcel_flags
        .groupby("block_id")[["intersection_area_sqm", "tra_area", "gg_area", "ppa_area", "toc_area"]]
        .sum()
        .reset_index()
    )
    for geo in ["tra", "gg", "ppa", "toc"]:
        block_geo[f"{geo}_area_share"] = block_geo[f"{geo}_area"] / block_geo["intersection_area_sqm"]

    flag_cols = ["parcel_id", "block_id", "parcel_block_share", "is_tra", "is_gg", "is_ppa", "is_toc"]
    return parcel_block_xwalk, parcel_flags[flag_cols].copy(), block_geo


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_superdistricts():
    """Print all available superdistrict names for use in create_parcel_block_map()."""
    _load_taz_xwalk()
    sd = _load_sd_names()
    names = sorted(sd["sd_name"].dropna().unique())
    print(f"{len(names)} superdistricts:")
    for n in names:
        print(f"  {n!r}")


def create_parcel_block_map(
    sd_names,
    geo: str = "TRA",
    output_path=None,
) -> folium.Map:
    """Create an interactive folium map of parcels classified by geo membership and block area share.

    Parameters
    ----------
    sd_names : str or list of str
        One or more superdistrict names (use list_superdistricts() to see valid values).
    geo : {"TRA", "GG", "PPA", "TOC"}
        The geography type to visualise.
    output_path : str or Path, optional
        If provided, saves the HTML map to this path.

    Returns
    -------
    folium.Map

    Classification categories
    -------------------------
    "{geo} parcel → {geo} block":     parcel is in geo AND block ≥50% qualifying   (blue)
    "{geo} parcel → non-{geo} block": parcel is in geo AND block <50% qualifying   (red)
    Out-of-geo parcels are not shown.
    """
    if isinstance(sd_names, str):
        sd_names = [sd_names]

    geo = geo.upper()
    if geo not in GEO_CONFIG:
        raise ValueError(f"geo must be one of {list(GEO_CONFIG)}, got {geo!r}")

    cfg       = GEO_CONFIG[geo]
    is_col    = cfg["is_col"]
    share_col = cfg["share_col"]

    # ---- resolve parcel set for requested superdistrict(s) ------------------
    _load_taz_xwalk()
    sd_df = _load_sd_names()

    parcel_sd = metrics_utils.parcel_taz_sd_crosswalk_df[["parcel_id", "superdistrict"]].copy()
    parcel_sd = parcel_sd.merge(sd_df, on="superdistrict", how="left")

    mask = parcel_sd["sd_name"].isin(sd_names)
    if not mask.any():
        available = sorted(sd_df["sd_name"].dropna().unique())
        raise ValueError(
            f"No parcels found for sd_names={sd_names!r}.\nAvailable: {available}"
        )
    target_parcels = set(parcel_sd.loc[mask, "parcel_id"])
    print(f"  {len(target_parcels):,} parcels in {sd_names}")

    # ---- classify parcels ---------------------------------------------------
    parcel_block_xwalk, parcel_flags, block_geo = _load_parcel_geo_data()

    # Dominant block per parcel (largest parcel_block_share)
    pb = parcel_block_xwalk[parcel_block_xwalk["parcel_id"].isin(target_parcels)].copy()
    dominant = pb.loc[
        pb.groupby("parcel_id")["parcel_block_share"].idxmax(),
        ["parcel_id", "block_id"],
    ].copy()

    # Join is_geo flag (one row per parcel)
    pf = (
        parcel_flags[parcel_flags["parcel_id"].isin(target_parcels)][["parcel_id", is_col]]
        .drop_duplicates("parcel_id")
    )
    dominant = dominant.merge(pf, on="parcel_id", how="left")

    # Join block area share
    dominant = dominant.merge(
        block_geo[["block_id", share_col]].rename(columns={share_col: "block_area_share"}),
        on="block_id", how="left",
    )
    dominant["block_area_share"] = dominant["block_area_share"].fillna(0.0)
    dominant["is_geo"] = dominant[is_col].fillna(False)

    # Build geo-specific class labels and color map
    label_in  = f"{geo} parcel \u2192 {geo} block"
    label_out = f"{geo} parcel \u2192 non-{geo} block"
    class_colors = {
        label_in:  "#1565C0",  # blue
        label_out: "#B71C1C",  # red
    }

    def _classify(is_geo, share):
        if not is_geo:
            return None  # out-of-geo parcels hidden
        return label_in if share >= HALF_AREA else label_out

    dominant["class"] = dominant.apply(lambda r: _classify(r["is_geo"], r["block_area_share"]), axis=1)
    dominant = dominant[dominant["class"].notna()].copy()
    print(f"  {len(dominant):,} in-{geo} parcels to render")

    # ---- load parcel geometries ---------------------------------------------
    print("  Loading parcel geometries (may take a moment)...")
    parcels_gdf = gpd.read_file(PARCELS_FILE).to_crs(ANALYSIS_CRS)
    parcels_gdf = parcels_gdf[["parcel_id", "geometry"]].copy()
    parcels_gdf = parcels_gdf[parcels_gdf["parcel_id"].isin(dominant["parcel_id"])]

    parcels_gdf = parcels_gdf.merge(
        dominant[["parcel_id", "block_id", "is_geo", "block_area_share", "class"]],
        on="parcel_id", how="inner",
    )
    parcels_gdf["block_area_pct"] = (parcels_gdf["block_area_share"] * 100).round(1)
    parcels_gdf["color"] = parcels_gdf["class"].map(class_colors)

    parcels_wgs84 = parcels_gdf.to_crs(4326)

    # ---- load block boundaries for the SD area ------------------------------
    print("  Loading block boundaries...")
    bbox = tuple(parcels_wgs84.total_bounds)  # (minx, miny, maxx, maxy) in WGS84
    blocks_gdf = gpd.read_file(BLOCKS_2020_FILE, bbox=bbox).to_crs(4326)
    blocks_gdf = blocks_gdf.rename(columns={"GEOID20": "block_id"})[["block_id", "geometry"]]
    blocks_gdf = blocks_gdf[blocks_gdf["block_id"].isin(set(dominant["block_id"]))].copy()
    blocks_gdf = blocks_gdf.merge(
        block_geo[["block_id", share_col]].rename(columns={share_col: "block_area_share"}),
        on="block_id", how="left",
    )
    blocks_gdf["block_area_share"] = blocks_gdf["block_area_share"].fillna(0.0)
    blocks_gdf["block_area_pct"] = (blocks_gdf["block_area_share"] * 100).round(1)

    blocks_gdf["stroke_color"] = BLOCK_BOUNDARY_COLOR

    # ---- build folium map ---------------------------------------------------
    centroid = parcels_wgs84.geometry.union_all().centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=13, tiles="CartoDB positron")

    # Legend (dynamic labels based on geo argument)
    legend_items = "".join(
        f"<i style='background:{c};width:12px;height:12px;display:inline-block;margin-right:6px;'></i>{label}<br>"
        for label, c in class_colors.items()
    )
    legend_items += (
        f"<i style='border:2px solid {BLOCK_BOUNDARY_COLOR};width:12px;height:12px;"
        f"display:inline-block;margin-right:6px;'></i>Block boundary<br>"
    )
    legend_html = (
        "<div style='position:fixed;bottom:30px;left:30px;z-index:9999;"
        "background:white;padding:10px;border:1px solid grey;font-size:12px;'>"
        f"<b>{geo} Parcel Classification</b><br>{legend_items}</div>"
    )
    m.get_root().html.add_child(folium.Element(legend_html))

    # Block boundaries (rendered first so parcels appear on top)
    fg_blocks = folium.FeatureGroup(name="Block boundaries", show=True)
    folium.GeoJson(
        blocks_gdf,
        style_function=lambda f: {
            "fillOpacity": 0.0,
            "color": f["properties"]["stroke_color"],
            "weight": 1.5,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["block_id", "block_area_pct"],
            aliases=["Block ID:", f"{geo} block %:"],
        ),
    ).add_to(fg_blocks)
    fg_blocks.add_to(m)

    # One FeatureGroup per class for layer toggling
    for class_label, color in class_colors.items():
        subset = parcels_wgs84[parcels_wgs84["class"] == class_label].copy()
        if subset.empty:
            continue
        fg = folium.FeatureGroup(name=class_label, show=True)
        folium.GeoJson(
            subset,
            style_function=lambda f, c=color: {
                "fillColor": c,
                "color": "#333333",
                "weight": 0.5,
                "fillOpacity": 0.65,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["parcel_id", "block_id", "block_area_pct", "class"],
                aliases=["Parcel ID:", "Block ID:", f"{geo} block %:", "Class:"],
            ),
        ).add_to(fg)
        fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        m.save(str(output_path))
        print(f"  Map saved to {output_path}")

    return m


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate an interactive parcel geography classification map."
    )
    parser.add_argument(
        "sd_names", nargs="*",
        help="One or more superdistrict names (quoted if they contain spaces). "
             "Omit to list available superdistricts.",
    )
    parser.add_argument(
        "--geo", default="TRA", choices=["TRA", "GG", "PPA", "TOC"],
        help="Geography type to visualise (default: TRA).",
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output HTML file path (default: map_<geo>_<sd>.html in current directory).",
    )
    args = parser.parse_args()

    if not args.sd_names:
        list_superdistricts()
    else:
        out = args.output
        if out is None:
            slug = "_".join(s.lower().replace("/", "-").replace(" ", "_") for s in args.sd_names)
            out = f"map_{args.geo.lower()}_{slug}.html"
        create_parcel_block_map(args.sd_names, geo=args.geo, output_path=out)
