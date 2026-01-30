import random
import string
import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_SALES_ORDERS

def sales_invoices(customers_df, branch_df):
    rows = []
    used = set()

    def gen_inv(branch_cd, date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"INV-{branch_cd}-{date.year}-{suf}"
            if nbr not in used:
                used.add(nbr)
                return nbr

    for _ in range(NUM_SALES_ORDERS):
        cust = random.choice(customers_df["CustomerCD"])
        br = random.choice(branch_df["BranchCD"])
        date = fake.date_between("-6M", "today")

        rows.append({
            "InvoiceNbr": gen_inv(br, date),
            "CustomerCD": cust,
            "InvoiceDate": date,
            "BranchCD": br
        })

    return pd.DataFrame(rows)
