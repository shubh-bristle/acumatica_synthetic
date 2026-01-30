import uuid
import random
import pandas as pd
from config.settings import NUM_ACCOUNTS

def accounts():
    account_types = ["Asset", "Liability", "Income", "Expense"]

    descriptions = {
        "Asset": "Asset account used to record company owned resources",
        "Liability": "Liability account used to record company obligations",
        "Income": "Income account used to record revenue",
        "Expense": "Expense account used to record costs"
    }

    rows = []

    for _ in range(NUM_ACCOUNTS):
        acc_type = random.choice(account_types)
        rows.append({
            "AccountCD": str(uuid.uuid4()),
            "Description": descriptions[acc_type],
            "Type": acc_type,
            "Active": True
        })

    return pd.DataFrame(rows)

