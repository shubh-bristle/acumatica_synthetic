import random
import string
import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_SALES_ORDERS

def sales_orders(customers_df, branch_df):
    rows = []
    used = set()

    def gen_order(branch_cd, date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"SO-{branch_cd}-{date.year}-{suf}"
            if nbr not in used:
                used.add(nbr)
                return nbr

    for _ in range(NUM_SALES_ORDERS):
        cust = random.choice(customers_df["CustomerCD"])
        br = random.choice(branch_df["BranchCD"])
        date = fake.date_between("-12M", "today")

        rows.append({
            "OrderNbr": gen_order(br, date),
            "OrderType": "SO",
            "CustomerCD": cust,
            "OrderDate": date,
            "BranchCD": br
        })

    return pd.DataFrame(rows)
