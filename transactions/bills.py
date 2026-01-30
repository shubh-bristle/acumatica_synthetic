import random
import string
import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_BILLS

def bills(vendors_df, branch_df):
    rows = []
    used = set()

    def gen_bill(branch_cd, date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"BILL-{branch_cd}-{date.year}-{suf}"
            if nbr not in used:
                used.add(nbr)
                return nbr

    for _ in range(NUM_BILLS):
        vend = random.choice(vendors_df["VendorCD"])
        br = random.choice(branch_df["BranchCD"])
        date = fake.date_between("-12M", "today")

        rows.append({
            "RefNbr": gen_bill(br, date),
            "VendorCD": vend,
            "BillDate": date,
            "BranchCD": br
        })

    return pd.DataFrame(rows)
