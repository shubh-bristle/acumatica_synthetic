import random
import string
import pandas as pd
from config.settings import NUM_STOCKITEMS

def stockitem(itemclass_df, uom_df):
    rows = []

    for _ in range(NUM_STOCKITEMS):
        ic = itemclass_df.sample(1).iloc[0]
        uom = uom_df.sample(1).iloc[0]["UOM"]

        code = "STK" + "".join(random.choices(string.ascii_uppercase, k=4))

        rows.append({
            "InventoryCD": code,
            "Description": ic["Description"],
            "ItemClassCD": ic["ItemClassCD"],
            "BaseUnit": uom,
            "InventoryAccountCD": ic["InventoryAccountCD"],
            "InventorySubAccountCD": ic["InventorySubAccountCD"],
            "Active": True
        })

    return pd.DataFrame(rows)
