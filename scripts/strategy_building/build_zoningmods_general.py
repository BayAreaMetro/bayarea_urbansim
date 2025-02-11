import pandas as pd
import numpy as np
import yaml
import argparse
from pathlib import Path


def apply_zoning_modifications(zoningmods, modifications):

    for mod in modifications:
        conditions = mod['conditions']
        category = mod['category']
        updates = mod['updates']
        mask_dict = {}

        # Start with all zoningmod rows selected
        mask = pd.Series(True, index=zoningmods.index)
        
        # Apply each filter condition (column-wise filtering - all need to be true)
        # (conditions are separate column - value pairs)
        for cond in conditions:
            mask &= zoningmods.eval(cond)

        # Set the component column updates for relevant mask records
        for upd_key,upd_val in updates.items():
            # Apply updates only to the filtered rows
            zoningmods.loc[mask, upd_key] = upd_val

        # mask_df = pd.concat(mask_dict,names=['category','condition','oid']).unstack('condition')

        # print(mask_df.all(axis=1).head())
        # for upd, val in updates.items():
        #     print(upd)
        #     print(val)

    return zoningmods

def load_yaml(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

def main(yaml_path):
    config = load_yaml(yaml_path)

    
    zoning_mod_cols = config['zoningmodcat_cols']
    pg_input_file = config['input_file']
    mods_output_file = config['output_file']
            
    print(f"Loading parcels geography from {pg_input_file}")
    pg = pd.read_csv(pg_input_file)
    
    # Assign concatenations of the component columns
    print(f"Assigning zoningmodcat col based on {zoning_mod_cols}")
    pg["zoningmodcat"] = (
        pg[zoning_mod_cols]
        .astype(str)
        .apply(lambda x: "".join(x), axis=1)
        .str.lower()
    )

    # Create zoningmods by grouping parcels_geog based on zoningmodcat
    print("Creating zoningmods df as template for mods")
    zoningmods = pg.groupby(["zoningmodcat"] + zoning_mod_cols, dropna=False).size().reset_index(name='count')

    # Apply zoning modifications
    print('Applying mods')
    zoningmods = apply_zoning_modifications(zoningmods, config['zoning_modifications'])

    # Columns required for BAUS purposes
    ancillary_cols = ['add_bldg', 'drop_bldg', 'dua_down', 'far_down', 'far_up', 'dua_up']

    # Identify missing columns and add them
    for col in set(ancillary_cols) - set(zoningmods.columns):
        zoningmods[col] = np.nan

    print(f"Zoning modifications saved to {mods_output_file}")
    zoningmods.to_csv(mods_output_file, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply zoning modifications from a YAML configuration.")
    parser.add_argument("-y", "--yaml_path", type=str, help="Path to the YAML configuration file")
    args = parser.parse_args()
    
    main(args.yaml_path)
