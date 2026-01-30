import random
import string
import pandas as pd
from config.settings import NUM_PROJECTS

def projects(customers_df, branch_df):
    project_descriptions = [
        "Enterprise ERP Implementation",
        "Warehouse Process Optimization",
        "Manufacturing Capacity Expansion",
        "Retail Store Rollout",
        "Supply Chain Integration",
        "Customer Portal Development",
        "Financial System Upgrade",
        "Quality Improvement Initiative",
        "New Product Introduction",
        "Operational Cost Reduction Program",
        "IT Infrastructure Modernization",
        "Data Analytics and Reporting Program",
        "Regulatory Compliance Project",
        "Service Delivery Transformation",
        "Research and Development Initiative"
    ]

    rows = []
    used_codes = set()

    def generate_project_cd(branch_cd):
        while True:
            suffix = ''.join(
                random.choices(string.ascii_uppercase + string.digits, k=4)
            )
            cd = f"PRJ-{branch_cd}-{suffix}"
            if cd not in used_codes:
                used_codes.add(cd)
                return cd

    for _ in range(NUM_PROJECTS):
        branch_cd = random.choice(branch_df["BranchCD"])

        rows.append({
            "ProjectCD": generate_project_cd(branch_cd),
            "Description": random.choice(project_descriptions),
            "CustomerCD": random.choice(customers_df["CustomerCD"]),
            "BranchCD": branch_cd,
            "Active": True
        })

    return pd.DataFrame(rows)
