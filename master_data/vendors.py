import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_VENDORS

def vendors(terms_df):
    rows = []

    for i in range(NUM_VENDORS):
        rows.append({
            "VendorCD": f"VEND{i+1:06}",
            "VendorName": fake.company(),
            "TermsID": terms_df.sample(1).iloc[0]["TermsID"],
            "Active": True
        })

    return pd.DataFrame(rows)
