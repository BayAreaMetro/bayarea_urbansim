from __future__ import print_function
import sys
import time
import orca
import pandas as pd
import numpy as np
from functools import reduce
from urbansim import accounts
from urbansim_defaults import utils
from six import StringIO
from urbansim.utils import misc
from baus.utils import add_buildings
from urbansim.developer import sqftproforma



@orca.step()
def accessory_units(year, buildings, parcels, accessory_units):
    add_units = accessory_units[str(year)]
    buildings_juris = misc.reindex(parcels.juris, buildings.parcel_id)
    res_buildings = buildings_juris[buildings.general_type == "Residential"]
    add_buildings = groupby_random_choice(res_buildings, add_units)
    add_buildings = pd.Series(add_buildings.index).value_counts()
    buildings.local.loc[add_buildings.index, "residential_units"] += add_buildings.values


@orca.table(cache=True)
def renter_protections(policy):
    if policy['renter_protections_enable']:
        df = pd.read_csv(os.path.join(renter_protections_relocation_rates.csv))
    return df


