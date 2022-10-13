from __future__ import print_function
import orca
import pandas as pd
import numpy as np
    

# THIS THEN will just update the config relocation rates
@orca.table(cache=True)
def renter_protections(policy):
    if policy['renter_protections_enable']:
        df = pd.read_csv(os.path.join(renter_protections_relocation_rates.csv))
    return df