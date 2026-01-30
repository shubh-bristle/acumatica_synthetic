import random
import pandas as pd
from config.settings import NUM_TERMS

def terms():
    days = [0, 7, 15, 30, 45, 60]
    rows = []

    for d in days[:NUM_TERMS]:
        rows.append({
            "TermsID": f"NET{d}",
            "Description": f"Net {d} Days",
            "DueDays": d,
            "Active": True
        })

    return pd.DataFrame(rows)
