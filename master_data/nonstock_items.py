import random
import string
import pandas as pd
from config.settings import NUM_NONSTOCK_ITEMS

def nonstock_items(itemclass_df, uom_df):
    rows = []

    for _ in range(NUM_NONSTOCK_ITEMS):
        ic = itemclass_df.sample(1).iloc[0]
        uom = uom_df.sample(1).iloc[0]["UOM"]

        code = "NS-" + "".join(random.choices(string.ascii_uppercase, k=5))

        rows.append({
            "InventoryCD": code,
            "Description": ic["Description"],
            "ItemClassCD": ic["ItemClassCD"],
            "BaseUnit": uom,
            "ExpenseAccountCD": ic["InventoryAccountCD"],
            "ExpenseSubAccountCD": ic["InventorySubAccountCD"],
            "Active": True
        })

    return pd.DataFrame(rows)
