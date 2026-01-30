import random
import string
import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_PURCHASE_ORDERS

def purchase_orders(vendors_df, branch_df):
    rows = []
    used = set()

    def gen_po(branch_cd, date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"PO-{branch_cd}-{date.year}-{suf}"
            if nbr not in used:
                used.add(nbr)
                return nbr

    for _ in range(NUM_PURCHASE_ORDERS):
        vend = random.choice(vendors_df["VendorCD"])
        br = random.choice(branch_df["BranchCD"])
        date = fake.date_between("-12M", "today")

        rows.append({
            "OrderNbr": gen_po(br, date),
            "OrderType": "PO",
            "VendorCD": vend,
            "OrderDate": date,
            "BranchCD": br
        })

    return pd.DataFrame(rows)
