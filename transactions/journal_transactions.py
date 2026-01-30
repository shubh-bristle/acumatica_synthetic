import random
import pandas as pd
from config.settings import NUM_JOURNAL_TRANSACTIONS
from utils.currency_utils import weighted_currency_choice
from utils.faker_setup import fake

def journal_transactions(ledger_df, branch_df, account_df, subaccount_df, currency_df):
    rows = []

    actual_ledgers = ledger_df[ledger_df["BalanceType"]=="Actual"]["LedgerCD"]

    for i in range(NUM_JOURNAL_TRANSACTIONS):
        debit = round(random.uniform(100, 5000), 2)

        rows.append({
            "BatchNbr": f"JB{i+1:06}",
            "LedgerCD": random.choice(actual_ledgers),
            "BranchCD": random.choice(branch_df["BranchCD"]),
            "AccountCD": random.choice(account_df["AccountCD"]),
            "SubAccountCD": random.choice(subaccount_df["SubAccountCD"]),
            "CurrencyID": weighted_currency_choice(currency_df),
            "DebitAmt": debit,
            "CreditAmt": debit,
            "TransactionDate": fake.date_between("-12M", "today"),
            "Active": True
        })

    return pd.DataFrame(rows)
