import pandas as pd
from config.settings import NUM_UOM

def unit_of_measure():
    uoms = ["EA", "HR", "DAY", "BOX", "KG", "LB", "LTR", "MTR"]

    return pd.DataFrame([{
        "UOM": u,
        "Description": u,
        "Active": True
    } for u in uoms[:NUM_UOM]])
