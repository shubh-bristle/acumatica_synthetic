import random
import string
import pandas as pd
from utils.faker_setup import fake

def shipments(sales_orders_df, warehouse_df):
    rows = []
    used = set()

    def gen_ship(date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"SHP-{date.year}-{suf}"
            if nbr not in used:
                used.add(nbr)
                return nbr

    for _, so in sales_orders_df.iterrows():
        ship_date = fake.date_between(so["OrderDate"], "today")

        rows.append({
            "ShipmentNbr": gen_ship(ship_date),
            "OrderNbr": so["OrderNbr"],
            "WarehouseCD": random.choice(warehouse_df["WarehouseCD"]),
            "ShipmentDate": ship_date
        })

    return pd.DataFrame(rows)
