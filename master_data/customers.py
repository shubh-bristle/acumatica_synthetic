import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_CUSTOMERS

def customers(customer_class_df, terms_df):
    rows = []

    for i in range(NUM_CUSTOMERS):
        rows.append({
            "CustomerCD": f"CUST{i+1:06}",
            "CustomerName": fake.company(),
            "CustomerClassID": customer_class_df.sample(1).iloc[0]["CustomerClassID"],
            "TermsID": terms_df.sample(1).iloc[0]["TermsID"],
            "Active": True
        })

    return pd.DataFrame(rows)
