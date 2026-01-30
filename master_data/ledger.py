import pandas as pd
from utils.currency_utils import weighted_currency_choice
from config.settings import NUM_LEDGERS

def ledger(currency_df):
    ledger_type_plan = (
        ["Actual"] * 30 +
        ["Budget"] * 30 +
        ["Statistical"] * 10 +
        ["Report"] * 10
    )[:NUM_LEDGERS]

    counter = {"Actual":1,"Budget":1,"Statistical":1,"Report":1}
    ledgers = []

    for t in ledger_type_plan:
        idx = counter[t]
        counter[t] += 1

        ledgers.append({
            "LedgerCD": f"{t[:3].upper()}_{idx:02}",
            "Description": f"{t} ledger",
            "BalanceType": t,
            "CurrencyID": weighted_currency_choice(currency_df) if t in ["Actual","Budget"] else None,
            "Active": True
        })

    return pd.DataFrame(ledgers)
