import random
import pandas as pd

def branch(ledger_df):
    actual_ledgers = ledger_df[ledger_df["BalanceType"]=="Actual"]["LedgerCD"].tolist()
    base = ["HQ","US_EAST","US_WEST","EMEA","APAC","LATAM","MFG","DC","STORE","SERVICES","RND","SHARED"]
    branches = []
    i = 1

    while len(branches) < 100:
        for b in base:
            if len(branches) >= 100:
                break
            branches.append({
                "BranchCD": f"{b}{i:02}",
                "Description": f"{b} Branch {i:02}",
                "LedgerCD": random.choice(actual_ledgers),
                "Active": True
            })
            i += 1

    return pd.DataFrame(branches)
