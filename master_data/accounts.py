import uuid
import random
import pandas as pd
from config.settings import NUM_ACCOUNTS

def accounts():
    account_types = ["Asset", "Liability", "Income", "Expense"]

    # Generate n unique random numbers starting from 100000
    ids = random.sample(range(100000, 100000 + NUM_ACCOUNTS*10), NUM_ACCOUNTS)

    descriptions = {
        "Asset": "Asset account used to record company owned resources",
        "Liability": "Liability account used to record company obligations",
        "Income": "Income account used to record revenue",
        "Expense": "Expense account used to record costs"
    }

    rows = []

    for acc_id in ids:
        acc_type = random.choice(account_types)

        rows.append({
            "AccountCD": acc_id,   # random unique ID
            "Description": descriptions[acc_type],
            "Type": acc_type,
            "Active": True
        })
        

    return pd.DataFrame(rows)

