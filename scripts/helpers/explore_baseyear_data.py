"""
BAUS H5 Data Exploration Utilities

This module provides functions to explore the BAUS base year data stored in HDF5 format.

Example Usage:
--------------

# Import the module
import explore_baus_data as eby

# 1. See what tables are available
eby.list_available_tables()

# 2. Get quick info about a table (without loading full data)
eby.quick_table_info('buildings')
eby.quick_table_info('households')
eby.quick_table_info('jobs')

# 3. Extract full tables for analysis
buildings = eby.extract_h5_table('buildings')
households = eby.extract_h5_table('households')
jobs = eby.extract_h5_table('jobs')

# 4. Extract samples for faster exploration
buildings_sample = eby.extract_h5_table('buildings', sample_rows=5000)
hh_sample = eby.extract_h5_table('households', sample_rows=10000)

# 5. Extract specific columns only
jobs_basic = eby.extract_h5_table('jobs', columns=['sector_id', 'building_id', 'taz'])
hh_demo = eby.extract_h5_table('households', 
                               columns=['building_id', 'persons', 'income', 'base_income_quartile'])

"""

import pandas as pd
import numpy as np
import os

def load_table_safely(h5_path, table_name):
    """Load a table with error handling for data type issues"""
    
    try:
        # First, try normal loading
        df = pd.read_hdf(h5_path, key=table_name)
        return df
        
    except ValueError as e:
        if "timedelta" in str(e).lower():
            print(f"  Data type issue detected in {table_name}, trying to fix...")
            
            # Try loading with different approaches
            try:
                # Method 1: Try to load with errors='coerce'
                with pd.HDFStore(h5_path, mode='r') as store:
                    df = store[table_name]
                    return df
            except:
                pass
            
            try:
                # Method 2: Use h5py to load and convert manually
                import h5py
                with h5py.File(h5_path, 'r') as f:
                    if table_name in f:
                        # This is complex - for now, skip problematic tables
                        print(f"  Unable to load {table_name} due to data type issues")
                        return None
            except:
                pass
        
        print(f"  Could not load {table_name}: {e}")
        return None
    
    except Exception as e:
        print(f"  Error loading {table_name}: {e}")
        return None



def extract_h5_table(table_name, sample_rows=None, columns=None):
    """
    Extract and return a specific table from the BAUS H5 file for interactive exploration.
    
    Parameters:
    -----------
    table_name : str
        Name of the table to extract. Available tables:
        'parcels', 'buildings_preproc', 'households_preproc', 'jobs_preproc', 
        'residential_units_preproc', 'zones'
    sample_rows : int, optional
        Number of rows to sample. If None, returns full table.
    columns : list, optional
        List of specific columns to return. If None, returns all columns.
    
    Returns:
    --------
    pandas.DataFrame
        The requested table as a DataFrame
    
    Examples:
    ---------
    # Get full buildings table
    buildings = extract_h5_table('buildings_preproc')
    
    # Get sample of households with specific columns
    hh_sample = extract_h5_table('households_preproc', sample_rows=1000, 
                                 columns=['income', 'persons', 'building_id'])
    
    # Get all jobs data
    jobs = extract_h5_table('jobs_preproc')
    """
    
    h5_path = r"M:\urban_modeling\baus\BAUS Inputs\basis_inputs\parcels_buildings_agents\2015_09_01_bayarea_v3.h5"
    
    # Available tables mapping
    available_tables = {
        'parcels': 'parcels',
        'buildings': 'buildings_preproc', 
        'buildings_preproc': 'buildings_preproc',
        'households': 'households_preproc',
        'households_preproc': 'households_preproc', 
        'jobs': 'jobs_preproc',
        'jobs_preproc': 'jobs_preproc',
        'residential_units': 'residential_units_preproc',
        'residential_units_preproc': 'residential_units_preproc',
        'zones': 'zones'
    }
    
    # Normalize table name
    if table_name not in available_tables:
        available = list(available_tables.keys())
        raise ValueError(f"Table '{table_name}' not found. Available tables: {available}")
    
    actual_table_name = available_tables[table_name]
    
    print(f"Loading table: {actual_table_name}")
    
    # Load the table safely
    df = load_table_safely(h5_path, actual_table_name)
    if df is None:
        return None
        
    # Apply column filter if specified
    if columns:
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            print(f"Warning: Columns not found: {missing_cols}")
            columns = [col for col in columns if col in df.columns]
        if columns:  # Only filter if we have valid columns
            df = df[columns]
    
    # Apply row sampling if specified
    original_len = len(df)
    if sample_rows and sample_rows < len(df):
        df = df.sample(n=sample_rows, random_state=42)
        print(f"Sampled {sample_rows:,} rows from {original_len:,} total rows")
    
    print(f"Extracted table shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    return df

def quick_table_info(table_name):
    """
    Get quick info about a table without loading the full data.
    
    Parameters:
    -----------
    table_name : str
        Name of the table to examine
    
    Returns:
    --------
    dict
        Dictionary with table info (shape, columns, memory usage)
    """
    
    h5_path = r"M:\urban_modeling\baus\BAUS Inputs\basis_inputs\parcels_buildings_agents\2015_09_01_bayarea_v3.h5"
    available_tables = {
        'parcels': 'parcels', 'buildings': 'buildings_preproc', 'buildings_preproc': 'buildings_preproc',
        'households': 'households_preproc', 'households_preproc': 'households_preproc', 
        'jobs': 'jobs_preproc', 'jobs_preproc': 'jobs_preproc',
        'residential_units': 'residential_units_preproc', 'residential_units_preproc': 'residential_units_preproc',
        'zones': 'zones'
    }
    
    if table_name not in available_tables:
        available = list(available_tables.keys())
        print(f"Table '{table_name}' not found. Available tables: {available}")
        return None
    
    actual_table_name = available_tables[table_name]
    
    try:
        # Load table safely
        df = load_table_safely(h5_path, actual_table_name)
        if df is None:
            return None
        
        info = {
            'table_name': table_name,
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': dict(df.dtypes),
            'index_name': df.index.name,
            'memory_mb': df.memory_usage(deep=True).sum() / 1024**2
        }
        
        print(f"Table: {table_name}")
        print(f"Shape: {info['shape'][0]:,} rows × {info['shape'][1]} columns")
        print(f"Memory: {info['memory_mb']:.1f} MB")
        print(f"Index: {info['index_name']}")
        print(f"Columns: {', '.join(info['columns'][:10])}{'...' if len(info['columns']) > 10 else ''}")
        
        return info
        
    except Exception as e:
        print(f"Error getting info for '{table_name}': {e}")
        return None

def list_available_tables():
    """List all available tables in the H5 file."""
    
    tables = {
        'parcels': 'Parcels with zoning and geographic attributes',
        'buildings_preproc': 'Buildings with residential units and job spaces', 
        'households_preproc': 'Household agents with demographics',
        'jobs_preproc': 'Job agents with sector information',
        'residential_units_preproc': 'Individual residential units',
        'zones': 'Transportation analysis zones'
    }
    
    print("Available BAUS tables:")
    print("=" * 50)
    for table, description in tables.items():
        # Also show short aliases
        alias = table.replace('_preproc', '')
        if alias != table:
            print(f"  {table:<25} (or '{alias}')")
        else:
            print(f"  {table:<25}")
        print(f"    {description}")
        print()
    
    print("Usage examples:")
    print("  buildings = extract_h5_table('buildings')")
    print("  hh_sample = extract_h5_table('households', sample_rows=5000)")
    print("  jobs_subset = extract_h5_table('jobs', columns=['sector_id', 'building_id'])")
    print("  quick_table_info('buildings')")

if __name__ == "__main__":
    print("BAUS H5 Data Exploration Functions")
    print("=" * 40)
    list_available_tables()