from __future__ import print_function
import orca
import pandas as pd
import numpy as np
    


@orca.table(cache=True)
def renter_protections(households, renter_protections_relocation_rates):
# update the relocation rates from the transition_relocation config
    df = pd.merge(households.to_frame(["zone_id", "base_income_quartile", "tenure"]), renter_protections_relocation_rates.local,
                  on=["zone_id", "base_income_quartile", "tenure"], how="left")
    df.index = households.index

    return df
