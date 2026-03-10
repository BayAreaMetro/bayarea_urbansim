#%%
import sys
import os
import getpass
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely

sys.path.insert(0, str(Path(__file__).parent))
import metrics_utils

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

METRICS_DIR  = metrics_utils.BOX_DIR / "Plan Bay Area 2050+/Performance and Equity/Plan Performance/Equity_Performance_Metrics/Final_Blueprint"

NP_DIR       = Path(r"M:\urban_modeling\baus\PBA50Plus\PBA50Plus_NoProject\PBA50Plus_NoProject_v38")
FBP_DIR      = Path(r"M:\urban_modeling\baus\PBA50Plus\PBA50Plus_FinalBlueprint\PBA50Plus_Final_Blueprint_v65")
CONTRAST_DIR = Path(r"M:\urban_modeling\baus\PBA50Plus\PBA50Plus_FinalBlueprint\PBA50Plus_CNTRST_NP_to_FBP_NP04_v4")
CLOUD_DIR    = Path(r"M:\urban_modeling\urbansim_cloud\projects\initial")
COMBO_OUT    = Path(r"M:\urban_modeling\urbansim_cloud\projects\combo")

PARCELS_FILE     = metrics_utils.BOX_DIR / "Modeling and Surveys/Urban Modeling/Spatial/Parcels/parcel_geoms_fgdb_out.shp"
BLOCKS_2020_FILE = Path(r"E:\Box\Modeling and Surveys\Census\2020\geo\bay_area_blocks_2020.gpkg")
ANALYSIS_CRS     = 26910
GEO_XWALK_METHOD = "overlay"  # "overlay_scaled": proportional by area_share, control-total preserving
                               # "overlay":        half-area rule (binary >= 0.5 threshold)
INTERPOLATE_TO_2023 = True    # If True, interpolate base year from 2020+2025 → 2023 (matches published metrics_growth.py)
BASE_YEAR = 2023 if INTERPOLATE_TO_2023 else 2020

# ---------------------------------------------------------------------------
# Build combo TAZ-level output
# Columns: model, variant, year, zone_id, tothh, totemp, resunits
# ---------------------------------------------------------------------------


OUTPUT_COLS       = ["model", "variant", "year", "zone_id", "tothh", "totemp", "resunits"]
BLOCK_OUTPUT_COLS = ["model", "variant", "year", "block_id", "tothh", "totemp"]
METRIC_COLS       = ["tothh", "totemp", "resunits"]
GROWTH_METRIC_COLS = ["tothh", "totemp"]   # resunits excluded from growth output files
GEO_COLS          = ["county", "superdistrict", "sd_name", "tra_label", "gg_label", "ppa_label", "toc_label", "epc_label"]

VARIANT_NAMES = {
    "np":       "No Project",
    "fbp":      "Final Blueprint",
    "contrast": "Contrast",
}

# File configs: one entry per model/variant/year combination
FILE_CONFIGS = [
    # BAUS – No Project
    {"model": "baus", "variant": "np", "year": 2020,
     "path": Path(NP_DIR, "travel_model_summaries", "PBA50Plus_NoProject_v38_taz1_summary_2020.csv")},
    {"model": "baus", "variant": "np", "year": 2050,
     "path": Path(NP_DIR, "travel_model_summaries", "PBA50Plus_NoProject_v38_taz1_summary_2050.csv")},
    # BAUS – Final Blueprint
    {"model": "baus", "variant": "fbp", "year": 2020,
     "path": Path(FBP_DIR, "travel_model_summaries", "PBA50Plus_Final_Blueprint_v65_taz1_summary_2020.csv")},
    {"model": "baus", "variant": "fbp", "year": 2050,
     "path": Path(FBP_DIR, "travel_model_summaries", "PBA50Plus_Final_Blueprint_v65_taz1_summary_2050.csv")},
    # BAUS – Contrast
    {"model": "baus", "variant": "contrast", "year": 2020,
     "path": Path(CONTRAST_DIR, "travel_model_summaries", "PBA50Plus_CNTRST_NP_to_FBP_NP04_v4_taz1_summary_2020.csv")},
    {"model": "baus", "variant": "contrast", "year": 2050,
     "path": Path(CONTRAST_DIR, "travel_model_summaries", "PBA50Plus_CNTRST_NP_to_FBP_NP04_v4_taz1_summary_2050.csv")},
    # Cloud – No Project (BAU)
    {"model": "cloud", "variant": "np", "year": 2020,
     "path": Path(CLOUD_DIR, "taz", "BAU w_ control totals (taz) - 2020.csv")},
    {"model": "cloud", "variant": "np", "year": 2050,
     "path": Path(CLOUD_DIR, "taz", "BAU w_ control totals (taz) - 2050.csv")},
    # Cloud – Final Blueprint (Alt Growth)
    {"model": "cloud", "variant": "fbp", "year": 2020,
     "path": Path(CLOUD_DIR, "taz", "Alt Growth W_ Control Totals  (taz) - 2020.csv")},
    {"model": "cloud", "variant": "fbp", "year": 2050,
     "path": Path(CLOUD_DIR, "taz", "Alt Growth W_ Control Totals  (taz) - 2050.csv")},
] + ([
    # 2025 files for base-year interpolation
    {"model": "baus",  "variant": "np",       "year": 2025,
     "path": Path(NP_DIR, "travel_model_summaries", "PBA50Plus_NoProject_v38_taz1_summary_2025.csv")},
    {"model": "baus",  "variant": "fbp",      "year": 2025,
     "path": Path(FBP_DIR, "travel_model_summaries", "PBA50Plus_Final_Blueprint_v65_taz1_summary_2025.csv")},
    {"model": "baus",  "variant": "contrast", "year": 2025,
     "path": Path(CONTRAST_DIR, "travel_model_summaries", "PBA50Plus_CNTRST_NP_to_FBP_NP04_v4_taz1_summary_2025.csv")},
    {"model": "cloud", "variant": "np",       "year": 2025,
     "path": Path(CLOUD_DIR, "taz", "BAU w_ control totals (taz) - 2025.csv")},
    {"model": "cloud", "variant": "fbp",      "year": 2025,
     "path": Path(CLOUD_DIR, "taz", "Alt Growth W_ Control Totals  (taz) - 2025.csv")},
] if INTERPOLATE_TO_2023 else [])

# Parcel-level file configs for block-level geography aggregation
BAUS_PARCEL_FILE_CONFIGS = [
    {"model": "baus", "variant": "np", "year": 2020,
     "path": NP_DIR / "core_summaries" / "PBA50Plus_NoProject_v38_parcel_summary_2020.csv"},
    {"model": "baus", "variant": "np", "year": 2050,
     "path": NP_DIR / "core_summaries" / "PBA50Plus_NoProject_v38_parcel_summary_2050.csv"},
    {"model": "baus", "variant": "fbp", "year": 2020,
     "path": FBP_DIR / "core_summaries" / "PBA50Plus_Final_Blueprint_v65_parcel_summary_2020.csv"},
    {"model": "baus", "variant": "fbp", "year": 2050,
     "path": FBP_DIR / "core_summaries" / "PBA50Plus_Final_Blueprint_v65_parcel_summary_2050.csv"},
    {"model": "baus", "variant": "contrast", "year": 2020,
     "path": CONTRAST_DIR / "core_summaries" / "PBA50Plus_CNTRST_NP_to_FBP_NP04_v4_parcel_summary_2020.csv"},
    {"model": "baus", "variant": "contrast", "year": 2050,
     "path": CONTRAST_DIR / "core_summaries" / "PBA50Plus_CNTRST_NP_to_FBP_NP04_v4_parcel_summary_2050.csv"},
] + ([
    {"model": "baus", "variant": "np",       "year": 2025,
     "path": NP_DIR / "core_summaries" / "PBA50Plus_NoProject_v38_parcel_summary_2025.csv"},
    {"model": "baus", "variant": "fbp",      "year": 2025,
     "path": FBP_DIR / "core_summaries" / "PBA50Plus_Final_Blueprint_v65_parcel_summary_2025.csv"},
    {"model": "baus", "variant": "contrast", "year": 2025,
     "path": CONTRAST_DIR / "core_summaries" / "PBA50Plus_CNTRST_NP_to_FBP_NP04_v4_parcel_summary_2025.csv"},
] if INTERPOLATE_TO_2023 else [])

CLOUD_BLOCK_FILE_CONFIGS = [
    {"model": "cloud", "variant": "np", "year": 2020,
     "path": CLOUD_DIR / "block" / "BAU w_ control totals (block) - 2020.csv"},
    {"model": "cloud", "variant": "np", "year": 2050,
     "path": CLOUD_DIR / "block" / "BAU w_ control totals (block) - 2050.csv"},
    {"model": "cloud", "variant": "fbp", "year": 2020,
     "path": CLOUD_DIR / "block" / "Alt Growth W_ Control Totals  (block) - 2020.csv"},
    {"model": "cloud", "variant": "fbp", "year": 2050,
     "path": CLOUD_DIR / "block" / "Alt Growth W_ Control Totals  (block) - 2050.csv"},
] + ([
    {"model": "cloud", "variant": "np",  "year": 2025,
     "path": CLOUD_DIR / "block" / "BAU w_ control totals (block) - 2025.csv"},
    {"model": "cloud", "variant": "fbp", "year": 2025,
     "path": CLOUD_DIR / "block" / "Alt Growth W_ Control Totals  (block) - 2025.csv"},
] if INTERPOLATE_TO_2023 else [])


def load_baus(path, model, variant, year):
    df = pd.read_csv(path)
    df = df.rename(columns={
        "ZONE":      "zone_id",
        "TOTHH":     "tothh",
        "TOTEMP":    "totemp",
        "RES_UNITS": "resunits",
    })
    df["model"]   = model
    df["variant"] = variant
    df["year"]    = year
    return df[OUTPUT_COLS]


def load_cloud(path, model, variant, year):
    df = pd.read_csv(path)
    df = df[df["geo_level_id"] != -1]   # drop zone -1
    df = df.rename(columns={
        "geo_level_id":          "zone_id",
        "sum_total_households":  "tothh",
        "sum_total_jobs":        "totemp",
        "sum_total_units":       "resunits",
    })
    df["model"]   = model
    df["variant"] = variant
    df["year"]    = year
    return df[OUTPUT_COLS]


def build_parcel_block20_xwalk():
    """Build a parcel_id -> block_id crosswalk using true polygon-on-polygon intersection.

    Parcels that straddle block boundaries produce multiple rows (one per block),
    weighted by parcel_block_share = fraction of that parcel's area in each block.
    Result saved to COMBO_OUT/parcel_block20_xwalk.csv.
    """
    cache_path = COMBO_OUT / "parcel_block20_xwalk.csv"
    print("Building parcel\u2192block20 crosswalk (polygon overlay, this may take a while)...")

    parcels_gdf = gpd.read_file(PARCELS_FILE).to_crs(ANALYSIS_CRS)
    parcels_gdf = parcels_gdf[["parcel_id", "geometry"]].copy()
    parcels_gdf.geometry = shapely.make_valid(parcels_gdf.geometry.values)

    # Load 2020 block polygons from disk (pre-fetched by fetch_2020_blocks.py)
    print("  Loading 2020 block geometries from disk...")
    blocks_gdf = gpd.read_file(BLOCKS_2020_FILE).to_crs(ANALYSIS_CRS)
    blocks_gdf = blocks_gdf.rename(columns={"GEOID20": "block_id"})[["block_id", "geometry"]]
    blocks_gdf.geometry = shapely.make_valid(blocks_gdf.geometry.values)

    # Polygon-on-polygon intersection (matches geo_assign_fields() methodology)
    print("  Running polygon overlay (parcel \u00d7 block)...")
    overlaid = gpd.overlay(parcels_gdf, blocks_gdf, how="intersection", make_valid=True)
    overlaid["intersection_area_sqm"] = overlaid.geometry.area

    # Fraction of each parcel's total area that falls in each block
    parcel_totals = overlaid.groupby("parcel_id")["intersection_area_sqm"].transform("sum")
    overlaid["parcel_block_share"] = overlaid["intersection_area_sqm"] / parcel_totals

    xwalk = overlaid[["parcel_id", "block_id", "intersection_area_sqm", "parcel_block_share"]].copy()

    xwalk.to_csv(cache_path, index=False)
    print(f"  Saved parcel\u2192block20 crosswalk: {len(xwalk):,} rows \u2192 {cache_path}")
    return xwalk


def build_block_geo_xwalk(parcel_block_xwalk, method=GEO_XWALK_METHOD):
    """Assign TRA/GG/PPA/TOC labels to each 2020 block using polygon intersection areas.

    method="overlay": half-area rule — a block is labeled TRA only if >=50% of the
        total parcel intersection area within that block belongs to qualifying parcels.
        Retains *_area_share columns for proportional scaling downstream.
    method="overlay_scaled": same area_share values used for proportional HH/emp
        splitting in scale_blocks_by_area_share().
    """
    geo_xwalk = metrics_utils.rtp2025_geography_crosswalk_df[
        ["PARCEL_ID", "tra_id", "gg_id", "ppa_id"]
    ].rename(columns={"PARCEL_ID": "parcel_id"}).copy()

    toc_xwalk = metrics_utils.rtp2025_parcel_toc_crosswalk_df[
        ["parcel_id", "toc_id"]
    ].copy()

    parcel_flags = (
        parcel_block_xwalk
        .merge(geo_xwalk, on="parcel_id", how="left")
        .merge(toc_xwalk, on="parcel_id", how="left")
    )
    parcel_flags["is_tra"] = parcel_flags["tra_id"].isin(["tra_1", "tra_2", "tra_3", "tra_4", "tra_5"])
    parcel_flags["is_gg"]  = parcel_flags["gg_id"]  == "GG"
    parcel_flags["is_ppa"] = parcel_flags["ppa_id"] == "PPA"
    parcel_flags["is_toc"] = parcel_flags["toc_id"] == "toc"

    # Area-weighted shares using true polygon intersection areas
    parcel_flags["tra_area"] = parcel_flags["is_tra"].astype(float) * parcel_flags["intersection_area_sqm"]
    parcel_flags["gg_area"]  = parcel_flags["is_gg"].astype(float)  * parcel_flags["intersection_area_sqm"]
    parcel_flags["ppa_area"] = parcel_flags["is_ppa"].astype(float) * parcel_flags["intersection_area_sqm"]
    parcel_flags["toc_area"] = parcel_flags["is_toc"].astype(float) * parcel_flags["intersection_area_sqm"]

    block_geo = (
        parcel_flags.groupby("block_id")[
            ["intersection_area_sqm", "tra_area", "gg_area", "ppa_area", "toc_area"]
        ]
        .sum()
        .reset_index()
    )
    block_geo["tra_area_share"] = block_geo["tra_area"] / block_geo["intersection_area_sqm"]
    block_geo["gg_area_share"]  = block_geo["gg_area"]  / block_geo["intersection_area_sqm"]
    block_geo["ppa_area_share"] = block_geo["ppa_area"] / block_geo["intersection_area_sqm"]
    block_geo["toc_area_share"] = block_geo["toc_area"] / block_geo["intersection_area_sqm"]

    block_geo["tra_label"] = np.where(block_geo["tra_area_share"] >= 0.5, "TRA",     "Non TRA")
    block_geo["gg_label"]  = np.where(block_geo["gg_area_share"]  >= 0.5, "GG",      "Non GG")
    block_geo["ppa_label"] = np.where(block_geo["ppa_area_share"] >= 0.5, "PPA",     "Non PPA")
    block_geo["toc_label"] = np.where(block_geo["toc_area_share"] >= 0.5, "TOC",     "Non TOC")
    return block_geo[["block_id", "tra_label", "gg_label", "ppa_label", "toc_label",
                       "tra_area_share", "gg_area_share", "ppa_area_share", "toc_area_share"]]


def scale_blocks_by_area_share(combo_blocks, geo_col, area_share_col):
    """Split each block into in/out-of-geography rows scaled by area_share.

    For a block with tra_area_share=0.6 and tothh=100:
      - TRA row:     tothh=60
      - Non TRA row: tothh=40

    Control totals are preserved exactly: in + out == original for every block-run.
    Requires combo_blocks to have *_area_share columns (overlay or overlay_scaled method).
    """
    pos_label = geo_col.replace("_label", "").upper()  # "TRA", "GG", "PPA", "TOC"
    neg_label = f"Non {pos_label}"

    share = combo_blocks[area_share_col].fillna(0)

    in_rows = combo_blocks.copy()
    in_rows[geo_col]  = pos_label
    in_rows["tothh"]  = combo_blocks["tothh"]  * share
    in_rows["totemp"] = combo_blocks["totemp"] * share

    out_rows = combo_blocks.copy()
    out_rows[geo_col]  = neg_label
    out_rows["tothh"]  = combo_blocks["tothh"]  * (1 - share)
    out_rows["totemp"] = combo_blocks["totemp"] * (1 - share)

    return pd.concat([in_rows, out_rows], ignore_index=True)


def load_baus_block(path, model, variant, year):
    """Load BAUS parcel summary; block aggregation happens downstream."""
    df = pd.read_csv(path, usecols=["parcel_id", "tothh", "totemp"])
    df["model"]   = model
    df["variant"] = variant
    df["year"]    = year
    return df


def load_cloud_block(path, model, variant, year):
    df = pd.read_csv(path)
    df = df[df["geo_level"] == "block"].copy()
    df = df.rename(columns={
        "geo_level_id":         "block_id",
        "sum_total_households": "tothh",
        "sum_total_jobs":       "totemp",
    })
    df["block_id"] = df["block_id"].astype(str).str.zfill(15)
    df["model"]    = model
    df["variant"]  = variant
    df["year"]     = year
    return df[BLOCK_OUTPUT_COLS]


def interpolate_to_base_year(df, id_col, numeric_cols, t1=2020, t2=2025, target=2023):
    """Linearly interpolate numeric columns from t1 and t2 to target year.

    Replaces t1 rows with interpolated target-year rows; drops t2 rows entirely.
    Matches metrics_utils.load_data_for_runs():
        val_target = val_t1 + ((target - t1) / (t2 - t1)) * (val_t2 - val_t1)
    """
    key_cols = ["model", "variant", id_col]
    weight = (target - t1) / (t2 - t1)

    rows_t2 = df[df["year"] == t2][key_cols + numeric_cols].rename(
        columns={c: f"{c}_t2" for c in numeric_cols}
    )
    result = df[df["year"] == t1].copy()
    result = result.merge(rows_t2, on=key_cols, how="left")
    for col in numeric_cols:
        result[col] = result[col] + weight * (result[f"{col}_t2"] - result[col])
        result.drop(columns=f"{col}_t2", inplace=True)
    result["year"] = target

    horizon = df[~df["year"].isin([t1, t2])].copy()
    return pd.concat([result, horizon], ignore_index=True)


frames = []
for cfg in FILE_CONFIGS:
    loader = load_cloud if cfg["model"] == "cloud" else load_baus
    frames.append(loader(cfg["path"], cfg["model"], cfg["variant"], cfg["year"]))

combo = pd.concat(frames, ignore_index=True)

# Verification: row counts per group
print(combo.groupby(["model", "variant", "year"]).size().to_string())

# ---------------------------------------------------------------------------
# TAZ crosswalk (county, superdistrict, sd_name — geo labels kept for block-level output only)
# ---------------------------------------------------------------------------


taz_xwalk = metrics_utils.load_taz_crosswalks(METRICS_DIR)

combo = combo.merge(
    taz_xwalk[["zone_id", "county", "superdistrict", "sd_name"]],
    how="left", on="zone_id", validate="many_to_one"
)
combo = combo[["zone_id"] + [c for c in combo.columns if c != "zone_id"]]

if INTERPOLATE_TO_2023:
    combo = interpolate_to_base_year(combo, "zone_id", ["tothh", "totemp", "resunits"])
else:
    combo = combo[combo["year"] != 2025].copy()

combo.to_csv(COMBO_OUT / "harmonized_runs.csv", index=False)
print(f"harmonized_runs.csv written: {len(combo):,} rows")

# ---------------------------------------------------------------------------
# Block-level output for geography calculations
# (parcel→block spatial xwalk + geo flags, then BAUS parcel agg + Cloud blocks)
# ---------------------------------------------------------------------------

parcel_block_xwalk = build_parcel_block20_xwalk()
block_geo_xwalk    = build_block_geo_xwalk(parcel_block_xwalk)

# Aggregate BAUS parcels to blocks
baus_parcel_frames = []
for cfg in BAUS_PARCEL_FILE_CONFIGS:
    baus_parcel_frames.append(load_baus_block(cfg["path"], cfg["model"], cfg["variant"], cfg["year"]))
baus_parcels = pd.concat(baus_parcel_frames, ignore_index=True)
baus_parcels = baus_parcels.merge(
    parcel_block_xwalk[["parcel_id", "block_id", "parcel_block_share"]],
    on="parcel_id", how="left"
)
# Scale tothh/totemp by the fraction of parcel area in each block (no-op for wholly-contained parcels)
baus_parcels["tothh"]  = baus_parcels["tothh"]  * baus_parcels["parcel_block_share"]
baus_parcels["totemp"] = baus_parcels["totemp"] * baus_parcels["parcel_block_share"]
baus_blocks = (
    baus_parcels.dropna(subset=["block_id"])
    .groupby(["model", "variant", "year", "block_id"])[["tothh", "totemp"]]
    .sum()
    .reset_index()
)

# Load Cloud block frames
cloud_block_frames = []
for cfg in CLOUD_BLOCK_FILE_CONFIGS:
    cloud_block_frames.append(load_cloud_block(cfg["path"], cfg["model"], cfg["variant"], cfg["year"]))
cloud_blocks = pd.concat(cloud_block_frames, ignore_index=True)

# Combine BAUS and Cloud blocks, attach geo labels
combo_blocks = pd.concat([baus_blocks, cloud_blocks], ignore_index=True)
if INTERPOLATE_TO_2023:
    combo_blocks = interpolate_to_base_year(combo_blocks, "block_id", ["tothh", "totemp"])
else:
    combo_blocks = combo_blocks[combo_blocks["year"] != 2025].copy()
combo_blocks = combo_blocks.merge(block_geo_xwalk, on="block_id", how="left")
print(f"combo_blocks: {len(combo_blocks):,} rows")
print(combo_blocks.groupby(["model", "variant", "year"]).size().to_string())

# ---------------------------------------------------------------------------
# Growth metrics by geography
# ---------------------------------------------------------------------------


def calc_growth(df, geo_col):
    """Return a DataFrame with one row per (model, variant, year, geo_col).
    Base-year (2020) rows have NaN for growth/share columns.
    Horizon-year (2050) rows carry computed growth and share-of-growth.
    """
    grouped = (
        df.groupby(["model", "variant", "year", geo_col])[GROWTH_METRIC_COLS]
        .sum()
        .reset_index()
    )

    base    = grouped[grouped["year"] == BASE_YEAR].copy()
    horizon = grouped[grouped["year"] == 2050].copy()

    # Compute growth on a merged frame so we can derive region shares
    merged = base.merge(
        horizon, on=["model", "variant", geo_col], suffixes=(f"_{BASE_YEAR}", "_2050")
    )
    merged["hh_growth"]   = merged["tothh_2050"]   - merged[f"tothh_{BASE_YEAR}"]
    merged["jobs_growth"] = merged["totemp_2050"]  - merged[f"totemp_{BASE_YEAR}"]

    region_totals = merged.groupby(["model", "variant"])[["hh_growth", "jobs_growth"]].transform("sum")
    merged["hh_share_of_growth"]   = merged["hh_growth"]   / region_totals["hh_growth"]
    merged["jobs_share_of_growth"] = merged["jobs_growth"] / region_totals["jobs_growth"]

    growth_info = merged[["model", "variant", geo_col,
                           "hh_growth", "jobs_growth",
                           "hh_share_of_growth", "jobs_share_of_growth"]]

    def _add_alias(row_df):
        row_df = row_df.copy()
        row_df["modelrun_alias"] = (
            row_df["year"].astype(str) + " " +
            row_df["variant"].map(VARIANT_NAMES)
        )
        return row_df

    # Base rows — growth cols are NaN.
    # 2020 base: all variants share the same observed starting point, emit only "np".
    # 2023 base: interpolated from variant-specific 2025 values, so variants differ — emit all.
    base_filter = base if INTERPOLATE_TO_2023 else base[base["variant"] == "np"]
    base_out = _add_alias(base_filter).rename(columns={"tothh": "total_households", "totemp": "total_jobs"})
    base_out["hh_growth"] = np.nan
    base_out["jobs_growth"] = np.nan
    base_out["hh_share_of_growth"] = np.nan
    base_out["jobs_share_of_growth"] = np.nan

    # Horizon rows — attach growth cols
    horizon_out = _add_alias(horizon).rename(columns={"tothh": "total_households", "totemp": "total_jobs"})
    horizon_out = horizon_out.merge(growth_info, on=["model", "variant", geo_col])

    col_order = ["model", "variant", "modelrun_alias", "year", geo_col,
                 "total_households", "total_jobs",
                 "hh_growth", "jobs_growth",
                 "hh_share_of_growth", "jobs_share_of_growth"]
    out = pd.concat([base_out, horizon_out], ignore_index=True)
    out = out[col_order].sort_values(["model", "variant", geo_col, "year"]).reset_index(drop=True)
    return out


def calc_jobs_housing_ratio(df):
    grouped = df.groupby(["model", "variant", "year", "county"])[METRIC_COLS].sum().reset_index()
    grouped["jobs_housing_ratio"] = grouped["totemp"] / grouped["tothh"]
    grouped["modelrun_alias"] = grouped["year"].astype(str) + " " + grouped["variant"].map(VARIANT_NAMES)
    return grouped[["model", "variant", "modelrun_alias", "year", "county", "tothh", "totemp", "jobs_housing_ratio"]]



# ---------------------------------------------------------------------------
# County growth
# ---------------------------------------------------------------------------
county_growth = calc_growth(combo, "county")
county_growth.to_csv(COMBO_OUT / "metrics_growthPattern_county_2050.csv", index=False)
print(f"metrics_growthPattern_county_2050.csv written: {len(county_growth):,} rows")

# ---------------------------------------------------------------------------
# Superdistrict growth (include sd_name)
# ---------------------------------------------------------------------------
sd_meta = taz_xwalk[["superdistrict", "sd_name", "county"]].drop_duplicates(subset="superdistrict")
sd_growth = calc_growth(combo, "superdistrict")
sd_growth = sd_growth.merge(sd_meta, on="superdistrict", how="left")
# Move sd_name and county next to superdistrict
cols = list(sd_growth.columns)
_sd_idx = cols.index("superdistrict")
cols.insert(_sd_idx + 1, cols.pop(cols.index("county")))
cols.insert(_sd_idx + 1, cols.pop(cols.index("sd_name")))
sd_growth = sd_growth[cols]
sd_growth.to_csv(COMBO_OUT / "metrics_growthPattern_superdistrict_2050.csv", index=False)
print(f"metrics_growthPattern_superdistrict_2050.csv written: {len(sd_growth):,} rows")

# ---------------------------------------------------------------------------
# Geographies growth (TRA, GG, PPA, TOC stacked with 'area' column)
# Block-level aggregation gives more accurate geo attribution than TAZ-level.
# EPC excluded — no block-level source available.
# ---------------------------------------------------------------------------
BINARY_GEO_COLS = ["tra_label", "gg_label", "ppa_label", "toc_label"]
geo_frames = []
for geo_col in BINARY_GEO_COLS:
    if GEO_XWALK_METHOD == "overlay_scaled":
        area_share_col = geo_col.replace("_label", "_area_share")
        blocks_for_geo = scale_blocks_by_area_share(combo_blocks, geo_col, area_share_col)
    else:
        blocks_for_geo = combo_blocks
    gdf = calc_growth(blocks_for_geo, geo_col).rename(columns={geo_col: "area"})
    geo_frames.append(gdf)
geo_growth = pd.concat(geo_frames, ignore_index=True)
# Reorder so 'area' follows modelrun_alias
_cols = list(geo_growth.columns)
_area_idx = _cols.index("area")
_cols.insert(3, _cols.pop(_area_idx))  # position 3 = after model, variant, modelrun_alias
geo_growth = geo_growth[_cols].sort_values(["model", "variant", "area", "year"]).reset_index(drop=True)
geo_growth.to_csv(COMBO_OUT / "metrics_growthPattern_geographies_2050.csv", index=False)
print(f"metrics_growthPattern_geographies_2050.csv written: {len(geo_growth):,} rows")

# ---------------------------------------------------------------------------
# Jobs/housing ratio
# ---------------------------------------------------------------------------
jobs_housing = calc_jobs_housing_ratio(combo)
jobs_housing.to_csv(COMBO_OUT / "jobs_housing_ratio.csv", index=False)
print(f"jobs_housing_ratio.csv written: {len(jobs_housing):,} rows")





# %%
