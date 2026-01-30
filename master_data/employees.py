import random
import pandas as pd
from utils.faker_setup import fake
from config.settings import NUM_EMPLOYEES

def employees(branch_df):
    branch_employee_distribution = {
        "HQ": 0.25,
        "US_EAST": 0.10,
        "US_WEST": 0.10,
        "EMEA": 0.10,
        "MFG01": 0.15,
        "DC01": 0.10,
        "STORE01": 0.08,
        "SERVICES": 0.06,
        "RND": 0.04,
        "SHARED": 0.02
    }

    rows = []
    counter = 1

    for _, branch in branch_df.iterrows():
        branch_cd = branch["BranchCD"]
        ratio = branch_employee_distribution.get(branch_cd, 0.02)
        count = max(1, int(NUM_EMPLOYEES * ratio))

        for _ in range(count):
            rows.append({
                "EmployeeCD": f"EMP{counter:06}",
                "EmployeeName": fake.name(),
                "BranchCD": branch_cd,
                "Active": True
            })
            counter += 1

    df = pd.DataFrame(rows)

    # Normalize to exactly NUM_EMPLOYEES
    if len(df) > NUM_EMPLOYEES:
        df = df.sample(NUM_EMPLOYEES).reset_index(drop=True)
    elif len(df) < NUM_EMPLOYEES:
        for _ in range(NUM_EMPLOYEES - len(df)):
            df = pd.concat([
                df,
                pd.DataFrame([{
                    "EmployeeCD": f"EMP{counter:06}",
                    "EmployeeName": fake.name(),
                    "BranchCD": random.choice(branch_df["BranchCD"]),
                    "Active": True
                }])
            ], ignore_index=True)
            counter += 1

    return df
