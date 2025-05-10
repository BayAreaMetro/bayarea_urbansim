import pandas as pd
import numpy as np
import yaml
import argparse
import logging
from pathlib import Path

def setup_logging():
    logging.basicConfig(
        filename="zoning_mods.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def apply_zoning_modifications(zoningmods, modifications):
    #overlap_check = {}
    for mod in modifications:
        conditions = mod["conditions"]
        category = mod["category"]
        updates = mod["updates"]

        mask = pd.Series(True, index=zoningmods.index)

        for cond in conditions:
            mask &= zoningmods.eval(cond)

        for upd_key, upd_val in updates.items():
            zoningmods.loc[mask, upd_key] = upd_val

        zoningmods.loc[mask,'yaml_category'] = category

        # overlap_check[category]=mask

        logging.info(f"Applied modification: {category} on {mask.sum()} rows.")
        logging.info

    # oc= pd.concat(overlap_check)
    # oc_count = oc[oc].unstack(0).sum(axis=1)
    # repeat_categories = oc_count[oc_count>1]
    # repeat_mods = zoningmods.iloc[repeat_categories.index][['zoningmodcat','yaml_category']]
    # logging.info(f'zoningmodcats addressed more than once by the filters: \n{repeat_mods}')

    return zoningmods

def baus_basis_dir():
    import os

    M_DRIVE = Path("/Volumes/Data/Models") if os.name != "nt" else Path("M:/")
    return M_DRIVE / "urban_modeling/baus/BAUS Inputs"


def load_yaml(yaml_path):
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)

        

def apply_inclusionary_modifications(zoningmods, incl_modifications):
    overlap_check = {}
    for mod in incl_modifications:
        conditions = mod["conditions"]
        category = mod["category"]
        val = mod["value"]

        # Initialize the mask as True for all rows
        mask = pd.Series(True, index=zoningmods.index)

        for cond in conditions:
            # Pass the string condition directly to eval, using the engine='python' argument
            mask &= zoningmods.eval(cond, engine='python')

        zoningmods.loc[mask, "inclusionary"] = val
        zoningmods.loc[mask, "inclusionary_category"] = category

        overlap_check[category]=mask.copy()

        zoningmods = zoningmods.sort_index()
        logging.info(f"Applied inclusionary modification: {category} on {mask.sum()} rows.")

    # instead of explicitly setting a filter for any remaining parcels - here's a fallback option for just setting 
    # unset records with a base .1 level

    remainder_mask = zoningmods.inclusionary.isna()
    logging.info(f'Setting remaining unset zoningmodcats to 10% for {len(remainder_mask)} records')
    zoningmods.loc[remainder_mask,['inclusionary_category','inclusionary']]= ['REMAINDER',.1]
    
    return zoningmods

def zoningmods_to_yaml(inclmods):
    def classify_setting(row):
        # Map float values to 'high', 'medium', 'low'
        value_to_setting = {0.2: "high", 0.15: "medium", 0.1: "low"}
        return value_to_setting.get(row["inclusionary"], "low")

    # Classify each zoningmodcat into a setting
    inclmods["setting"] = inclmods.apply(classify_setting, axis=1)

    # Prepare the YAML structure
    inclusionary_housing_settings = {
        "inclusionary_strategy": []
    }

    for (setting, amount), setting_group in inclmods.groupby(["setting", "inclusionary"]):
        inclusionary_housing_settings["inclusionary_strategy"].append({
            "type": "zoningmodcat",
            "description": f"{setting} setting",
            "amount": float(amount),  # Ensure amount is a native Python float
            "values": setting_group["zoningmodcat"].tolist()
        })

    return yaml.dump({"inclusionary_housing_settings": inclusionary_housing_settings}, default_flow_style=False)

# def mods_to_shape():
#     import geopandas as gpd
#     urbansim_parcels_topo_fix = gpd.read_parquet(BOX_DIR / 'Modeling and Surveys' / 'Urban Modeling' /
#                                              'Bay Area UrbanSim' / 'BASIS' / 'PBA50Plus' / 'urbansim_parcels_topo_fix.parquet')

#     urbansim_parcels_topo_fix = urbansim_parcels_topo_fix.set_index('parcel_id')
#     urbansim_parcels_topo_fix = urbansim_parcels_topo_fix.to_crs('EPSG:26910')
#     urbansim_parcels_topo_fix['geom_pt'] = urbansim_parcels_topo_fix.geometry.representative_point()

def main(yaml_path, input_file, mods_output_file, incl_output_yaml_file, apply_inclusionary):
    setup_logging()
    logging.info("Starting zoning modification process.")

    config = load_yaml(yaml_path)

    zoning_mod_cols = config["zoningmodcat_cols"]
    basis_dir = baus_basis_dir()

    logging.info(f"Loading parcels geography from {input_file}")
    pg = pd.read_csv(basis_dir / input_file)

    logging.info(f"Assigning zoningmodcat col based on {zoning_mod_cols}")
    pg["zoningmodcat"] = (
        pg[zoning_mod_cols]
        .astype(str)
        .apply(lambda x: "".join(x), axis=1)
        .str.lower()
    )

    pg[zoning_mod_cols] = pg[zoning_mod_cols].astype(str)#.str.upper().replace('NAN','nan')
    for col in zoning_mod_cols:
        pg[col]=pg[col].str.lower()
    
    zoningmods = pg.groupby(["zoningmodcat"] + zoning_mod_cols).size().reset_index(name="count")

    logging.info("Applying zoning modifications.")
    zoningmods = apply_zoning_modifications(zoningmods, config["zoning_modifications"])

    zoningmods.loc[:, zoning_mod_cols] = zoningmods.loc[:, zoning_mod_cols].replace("nan", np.nan)

    # full list of cols that should be present in zoning mods file
    ancillary_cols = ["add_bldg", "drop_bldg", "dua_down", "far_down", "far_up", "dua_up"]

    # if not present, add, set to nan
    for col in set(ancillary_cols) - set(zoningmods.columns):
        zoningmods[col] = np.nan

    zoningmods.to_csv(basis_dir / mods_output_file, index=False)
    logging.info(f"Zoning modifications saved to {mods_output_file}")

    if apply_inclusionary:
        logging.info("Applying inclusionary modifications.")
        inclmods = apply_inclusionary_modifications(zoningmods, config["inclusionary"])
        logging.info(f"Inclusionary modifications applied: {len(inclmods)} rows.")
        inclmods = inclmods[["zoningmodcat", "inclusionary", "inclusionary_category"]].dropna()
        logging.info("Pouring into a yaml file.")
        
        # Pour into a YAML structure
        yaml_output = zoningmods_to_yaml(inclmods)

        logging.info("Save the YAML output to a file")
        with open(basis_dir / incl_output_yaml_file, "w") as file:
            file.write(yaml_output)

        print("YAML output saved to inclusionary_housing_settings.yaml")
 
    logging.info("Process completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply zoning modifications from a YAML configuration.")
    parser.add_argument("-y", "--yaml_path", type=str, required=True, help="Path to the YAML configuration file")
    parser.add_argument("-i", "--pg_input_file", type=str, required=True, help="Input Growth Geography CSV file")
    parser.add_argument("-o", "--mods_output_file", type=str, required=True, help="Output Zoning Mods CSV file")
    parser.add_argument("-f", "--incl_output_yaml_file", type=str, required=True, help="Output Inclusionary yaml file")
    parser.add_argument("--apply_inclusionary", action="store_true", help="Apply inclusionary modifications (optional)")
    args = parser.parse_args()

    main(args.yaml_path, args.pg_input_file, args.mods_output_file, args.incl_output_yaml_file, args.apply_inclusionary)


