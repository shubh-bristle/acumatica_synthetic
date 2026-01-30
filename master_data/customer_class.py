import pandas as pd
import random
from utils.faker_setup import fake
from config.settings import NUM_CUSTOMER_CLASSES



def customer_class(accounts_df, subaccounts_df, terms_df):
    rows = []

    for i in range(NUM_CUSTOMER_CLASSES):
        rows.append({
            "CustomerClassID": f"CC{i+1:02}",
            "Description": "Standard customer classification",
            # "ARAccountCD": random.choice(accounts_df["AccountCD"]),
            # "ARSubAccountCD": random.choice(subaccounts_df["SubAccountCD"]),
            # "TermsID": random.choice(terms_df["TermsID"]),
            "Active": True
        })

    return pd.DataFrame(rows)

