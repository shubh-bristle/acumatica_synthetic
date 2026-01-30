import uuid
import pandas as pd
from config.settings import NUM_SUBACCOUNTS

def subaccounts():
    descriptions = [
        "Marketing Department",
        "Sales Department",
        "IT Department",
        "Finance Department",
        "Operations Department"
    ]

    rows = []

    for i in range(NUM_SUBACCOUNTS):
        rows.append({
            "SubAccountCD": str(uuid.uuid4()),
            "Description": descriptions[i % len(descriptions)],
            "Active": True
        })

    return pd.DataFrame(rows)
