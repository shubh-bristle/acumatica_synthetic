import random
import string
import pandas as pd
from utils.faker_setup import fake

def purchase_receipts(purchase_orders_df, warehouse_df):
    rows = []
    used = set()

    def gen_pr(date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"PR-{date.year}-{suf}"
            if nbr not in used:
                used.add(nbr)
                return nbr

    for _, po in purchase_orders_df.iterrows():
        date = fake.date_between(po["OrderDate"], "today")

        rows.append({
            "ReceiptNbr": gen_pr(date),
            "OrderNbr": po["OrderNbr"],
            "VendorCD": po["VendorCD"],
            "WarehouseCD": random.choice(warehouse_df["WarehouseCD"]),
            "ReceiptDate": date
        })

    return pd.DataFrame(rows)
