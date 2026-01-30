import random
import uuid
import pandas as pd
from config.settings import NUM_ITEMCLASSES

def itemclass(accounts_df, subaccounts_df):
    categories = [
        "Finished Goods",
        "Raw Materials",
        "Spare Parts",
        "Office Supplies"
    ]

    rows = []

    for _ in range(NUM_ITEMCLASSES):
        rows.append({
            "ItemClassCD": str(uuid.uuid4()),
            "Description": random.choice(categories),
            "InventoryAccountCD": random.choice(accounts_df["AccountCD"]),
            "InventorySubAccountCD": random.choice(subaccounts_df["SubAccountCD"]),
            "Active": True
        })

    return pd.DataFrame(rows)
