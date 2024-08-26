from __future__ import print_function

import json

import orca
import numpy as np
import pandas as pd
from urbansim.models import RegressionModel, SegmentedRegressionModel, \
    MNLDiscreteChoiceModel, SegmentedMNLDiscreteChoiceModel, \
    GrowthRateTransition, transition
from urbansim.models.supplydemand import supply_and_demand
from urbansim.developer import sqftproforma, developer
from urbansim.utils import misc


def check_nas(df):
    """
    Checks for nas and errors if they are found (also prints a report on how
    many nas are found in each column)

    Parameters
    ----------
    df : DataFrame
        DataFrame to check for nas

    Returns
    -------
    Nothing
    """
    df_cnt = len(df)
    fail = False

    df = df.replace([np.inf, -np.inf], np.nan)
    for col in df.columns:
        s_cnt = df[col].count()
        if df_cnt != s_cnt:
            fail = True
            print("Found %d nas or inf (out of %d) in column %s" % \
                  (df_cnt-s_cnt, df_cnt, col))

    assert not fail, "NAs were found in dataframe, please fix"


def to_frame(tbl, join_tbls, cfg, additional_columns=[]):
    """
    Leverage all the built in functionality of the sim framework to join to
    the specified tables, only accessing the columns used in cfg (the model
    yaml configuration file), an any additionally passed columns (the sim
    framework is smart enough to figure out which table to grab the column
    off of)

    Parameters
    ----------
    tbl : DataFrameWrapper
        The table to join other tables to
    join_tbls : list of DataFrameWrappers or strs
        A list of tables to join to "tbl"
    cfg : str
        The filename of a yaml configuration file from which to parse the
        strings which are actually used by the model
    additional_columns : list of strs
        A list of additional columns to include

    Returns
    -------
    A single DataFrame with the index from tbl and the columns used by cfg
    and any additional columns specified
    """
    join_tbls = join_tbls if isinstance(join_tbls, list) else [join_tbls]
    tables = [tbl] + join_tbls
    cfg = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)
    tables = [t for t in tables if t is not None]
    columns = misc.column_list(tables, cfg.columns_used()) + additional_columns
    if len(tables) > 1:
        df = orca.merge_tables(target=tables[0].name,
                               tables=tables, columns=columns)
    else:
        df = tables[0].to_frame(columns)
    check_nas(df)
    return df


def yaml_to_class(cfg):
    """
    Convert the name of a yaml file and get the Python class of the model
    associated with the configuration

    Parameters
    ----------
    cfg : str
        The name of the yaml configuration file

    Returns
    -------
    Nothing
    """
    import yaml
    model_type = yaml.safe_load(open(cfg))["model_type"]
    return {
        "regression": RegressionModel,
        "segmented_regression": SegmentedRegressionModel,
        "discretechoice": MNLDiscreteChoiceModel,
        "segmented_discretechoice": SegmentedMNLDiscreteChoiceModel
    }[model_type]


def _print_number_unplaced(df, fieldname):
    print("Total currently unplaced: %d" % \
          df[fieldname].value_counts().get(-1, 0))



def lcm_simulate(cfg, choosers, buildings, join_tbls, out_fname,
                 supply_fname, vacant_fname,
                 enable_supply_correction=None, cast=False,
                 alternative_ratio=2.0,
                 debug=True,
                 run_name='test', # temp
                 year=2010, 
                 outputs_dir='.'): 
    """
    Simulate the location choices for the specified choosers

    Parameters
    ----------
    cfg : string
        The name of the yaml config file from which to read the location
        choice model
    choosers : DataFrameWrapper
        A dataframe of agents doing the choosing
    buildings : DataFrameWrapper
        A dataframe of buildings which the choosers are locating in and which
        have a supply
    join_tbls : list of strings
        A list of land use dataframes to give neighborhood info around the
        buildings - will be joined to the buildings using existing broadcasts.
    out_fname : string
        The column name to write the simulated location to
    supply_fname : string
        The string in the buildings table that indicates the amount of
        available units there are for choosers, vacant or not
    vacant_fname : string
        The string in the buildings table that indicates the amount of vacant
        units there will be for choosers
    enable_supply_correction : Python dict
        Should contain keys "price_col" and "submarket_col" which are set to
        the column names in buildings which contain the column for prices and
        an identifier which segments buildings into submarkets
    cast : boolean
        Should the output be cast to match the existing column.
    alternative_ratio : float, optional
        Value to override the setting in urbansim.models.dcm.predict_from_cfg.
        Above this ratio of alternatives to choosers (default of 2.0), the
        alternatives will be sampled to improve computational performance
    debug : boolean, optional
        Value to set whether to output SegmentedMNLDiscreteChoiceModel.predict_from_cfg. 
        If debug is set to true, will set the variable “sim_pdf” on the object to store 
        the probabilities for mapping of the outcome.

    """
    import os
    cfg = misc.config(cfg)

    choosers_df = to_frame(choosers, [], cfg, additional_columns=[out_fname])

    additional_columns = [supply_fname, vacant_fname]
    if enable_supply_correction is not None and \
            "submarket_col" in enable_supply_correction:
        additional_columns += [enable_supply_correction["submarket_col"]]
    if enable_supply_correction is not None and \
            "price_col" in enable_supply_correction:
        additional_columns += [enable_supply_correction["price_col"]]
    locations_df = to_frame(buildings, join_tbls, cfg,
                            additional_columns=additional_columns+['parcel_id'])

    print(f"     elcm: head of locations_df (shape {locations_df.shape}): {locations_df.head()}")
    
    # note these come from buildings, not locations_df
    # available_units = buildings[supply_fname]
    # vacant_units = buildings[vacant_fname]

    # print("There are %d total available units" % available_units.sum())
    # print("    and %d total choosers" % len(choosers))
    # print("    but there are %d overfull buildings" % \
    #       len(vacant_units[vacant_units < 0]))

    # vacant_units = vacant_units[vacant_units > 0]
    # print(f"     elcm: head of vacant_units (shape {vacant_units.shape}): {vacant_units.head()}")
    
    # # sometimes there are vacant units for buildings that are not in the
    # # locations_df, which happens for reasons explained in the warning below
    # # question: if they don't join - SHOULD we include them at all?
    
    # # explodes to fully enumerated vacant job spaces
    # vacant_index_enum = np.repeat(vacant_units.index.values,
    #                     vacant_units.values.astype('int'))
    
    # # are the building_ids also in the locations_df ?
    # isin = pd.Series(vacant_index_enum).isin(locations_df.index)
    
    # # if not - we don't use
    # # q: why not just get vacant_units from location_df?
    # vacant_index_enum = vacant_index_enum[isin]

    # compare with this simplification:

    # note these come from buildings, not locations_df
    available_units = locations_df[supply_fname]
    vacant_units = locations_df[vacant_fname]


    print("There are %d total available units" % available_units.sum())
    print("    and %d total choosers" % len(choosers))
    print("    but there are %d overfull buildings" % \
          len(vacant_units[vacant_units < 0]))

    # subset rows to relevant buildings
    vacant_units = locations_df.loc[locations_df[vacant_fname]>0,vacant_fname]

    print("    for a total of %d temporarily empty units" % vacant_units.sum())
    print("    in %d buildings total in the region" % len(vacant_units))

    # explodes relevant buildings to fully enumerated vacant job spaces
    vacant_units_enum = vacant_units.index.repeat(vacant_units)
    units = locations_df.loc[vacant_units_enum].reset_index()

    
    #missing = len(isin[isin == False])
    
    # this repeats the building for each (vacant) job space
    #units = locations_df.loc[vacant_index_enum].reset_index()
    print(f"     elcm: head of units (shape {units.shape}): {units.head()}")
    check_nas(units)



    # if missing > 0:
    #     print("WARNING: %d indexes aren't found in the locations df -" % \
    #         missing)
    #     print("    this is usually because of a few records that don't join ")
    #     print("    correctly between the locations df and the aggregations tables")

    movers = choosers_df[choosers_df[out_fname] == -1]
    print("There are %d total movers for this LCM" % len(movers))

    if enable_supply_correction is not None:
        assert isinstance(enable_supply_correction, dict)
        assert "price_col" in enable_supply_correction
        price_col = enable_supply_correction["price_col"]
        assert "submarket_col" in enable_supply_correction
        submarket_col = enable_supply_correction["submarket_col"]

        lcm = yaml_to_class(cfg).from_yaml(str_or_buffer=cfg)

        if enable_supply_correction.get("warm_start", False) is True:
            raise NotImplementedError()

        multiplier_func = enable_supply_correction.get("multiplier_func", None)
        if multiplier_func is not None:
            multiplier_func = orca.get_injectable(multiplier_func)

        kwargs = enable_supply_correction.get('kwargs', {})
        new_prices, submarkets_ratios = supply_and_demand(
            lcm,
            movers,
            units,
            submarket_col,
            price_col,
            base_multiplier=None,
            multiplier_func=multiplier_func,
            **kwargs)

        # we will only get back new prices for those alternatives
        # that pass the filter - might need to specify the table in
        # order to get the complete index of possible submarkets
        submarket_table = enable_supply_correction.get("submarket_table", None)
        if submarket_table is not None:
            submarkets_ratios = submarkets_ratios.reindex(
                orca.get_table(submarket_table).index).fillna(1)
            # write final shifters to the submarket_table for use in debugging
            orca.get_table(submarket_table)["price_shifters"] = submarkets_ratios

        print("Running supply and demand")
        print("Simulated Prices")
        print(buildings[price_col].describe())
        print("Submarket Price Shifters")
        print(submarkets_ratios.describe())
        # we want new prices on the buildings, not on the units, so apply
        # shifters directly to buildings and ignore unit prices
        orca.add_column(buildings.name,
                        price_col+"_hedonic", buildings[price_col])
        new_prices = buildings[price_col] * \
            submarkets_ratios.loc[buildings[submarket_col]].values
        buildings.update_col_from_series(price_col, new_prices)
        print("Adjusted Prices")
        print(buildings[price_col].describe())

    if len(movers) > vacant_units.sum():
        print("WARNING: Not enough locations for movers")
        print("    reducing locations to size of movers for performance gain")
        movers = movers.head(int(vacant_units.sum()))

    this_model = yaml_to_class(cfg)
    chosen_units, dbg = this_model.predict_from_cfg(choosers=movers, alternatives=units, 
                                                 cfgname=cfg, 
                                                 alternative_ratio=alternative_ratio,
                                                 debug=debug)

    # debug=True is supposed to set an object variable, but it's not there:
    # per https://udst.github.io/urbansim/models/statistical.html
    # "If debug is set to true, will set the variable “sim_pdf” 
    # on the object to store the probabilities for mapping of the outcome."                            

    choice_probs = dbg.probabilities(movers,units) 
    choice_probs_wide = pd.concat(choice_probs).unstack(0)
    choice_probs_wide_by_segment = choice_probs_wide.groupby(level=1).sum()

    # chosen_units returns nans when there aren't enough units,
    # get rid of them and they'll stay as -1s
    chosen_units = chosen_units.dropna()

    # go from units back to buildings
    chosen_buildings = pd.Series(units.loc[chosen_units.values][out_fname].values,
                              index=chosen_units.index)

    choosers.update_col_from_series(out_fname, chosen_buildings, cast=cast)
    _print_number_unplaced(choosers, out_fname)

    if enable_supply_correction is not None:
        new_prices = buildings[price_col]
        if "clip_final_price_low" in enable_supply_correction:
            new_prices = new_prices.clip(lower=enable_supply_correction[
                "clip_final_price_low"])
        if "clip_final_price_high" in enable_supply_correction:
            new_prices = new_prices.clip(upper=enable_supply_correction[
                "clip_final_price_high"])
        buildings.update_col_from_series(price_col, new_prices)

    vacant_units = buildings[vacant_fname]
    print("    and there are now %d empty units" % vacant_units.sum())
    print("    and %d overfull buildings" % len(vacant_units[vacant_units < 0]))


    # for debugging purposes

    outputs_dir = os.path.join(outputs_dir,'debug')
    os.makedirs(outputs_dir,exist_ok=True)
    
    probs_out_path = os.path.join(outputs_dir,  f"{run_name}_{year}_DEBUG_elcm_probs.csv")
    locations_out_path = os.path.join(outputs_dir,  f"{run_name}_{year}_DEBUG_locations.csv")
    
    choice_probs_wide_by_segment.to_csv(probs_out_path)
    locations_df.to_csv(locations_out_path)


    # lastly - get probabilities not just for vacant units but for all units.
    # limited choosers, all units
    available_units_enum = available_units.index.repeat(available_units)
    units = locations_df.loc[available_units_enum].reset_index()

    choosers = choosers_df
    this_model = yaml_to_class(cfg)
    chosen_units, dbg = this_model.predict_from_cfg(choosers=movers, alternatives=units, 
                                                 cfgname=cfg, 
                                                 alternative_ratio=alternative_ratio,
                                                 debug=debug)


    choice_probs = dbg.probabilities(movers,units) 
    choice_probs_wide = pd.concat(choice_probs).unstack(0)
    choice_probs_wide_by_segment = choice_probs_wide.groupby(level=1).sum()

    #outputs_dir = os.path.join(outputs_dir,'debug')
    #os.makedirs(outputs_dir,exist_ok=True)
    
    probs_out_path = os.path.join(outputs_dir,  f"{run_name}_{year}_DEBUG_elcm_probs_select_choosers_all_units.csv")
    #locations_out_path = os.path.join(outputs_dir,  f"{run_name}_{year}_DEBUG_locations.csv")
    
    choice_probs_wide_by_segment.to_csv(probs_out_path)
    locations_df.to_csv(locations_out_path)

    # all choosers, all units
    chosen_units, dbg = this_model.predict_from_cfg(choosers=choosers_df, alternatives=units, 
                                                 cfgname=cfg, 
                                                 alternative_ratio=alternative_ratio,
                                                 debug=debug)


    choice_probs = dbg.probabilities(choosers,units) 
    choice_probs_wide = pd.concat(choice_probs).unstack(0)
    choice_probs_wide_by_segment = choice_probs_wide.groupby(level=1).sum()

    #outputs_dir = os.path.join(outputs_dir,'debug')
    #os.makedirs(outputs_dir,exist_ok=True)
    
    probs_out_path = os.path.join(outputs_dir,  f"{run_name}_{year}_DEBUG_elcm_probs_all_choosers_all_units.csv")
    #locations_out_path = os.path.join(outputs_dir,  f"{run_name}_{year}_DEBUG_locations.csv")
    
    choice_probs_wide_by_segment.to_csv(probs_out_path)
    locations_df.to_csv(locations_out_path)

    return chosen_units

