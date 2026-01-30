import random
import pandas as pd

def warehouse(branch_df):
    warehouses = []
    counter = 1

    branch_description_map = {
        "HQ": [
            "Primary warehouse supporting headquarters operations",
            "Central storage facility for corporate inventory"
        ],
        "DC": [
            "Primary distribution center warehouse",
            "Regional shipping and receiving warehouse"
        ],
        "MFG": [
            "Manufacturing raw materials warehouse",
            "Manufacturing finished goods warehouse"
        ],
        "STORE": [
            "Retail backroom inventory warehouse",
            "Retail overflow stock warehouse"
        ]
    }

    default_descriptions = [
        "Primary warehouse supporting branch operations",
        "Secondary warehouse supporting branch operations",
        "General purpose storage warehouse"
    ]

    for _, br in branch_df.iterrows():
        branch_cd = br["BranchCD"]
        branch_type = "".join(c for c in branch_cd if not c.isdigit())
        descriptions = branch_description_map.get(branch_type, default_descriptions)

        for _ in range(random.randint(1, 3)):
            if counter > 200:
                break

            warehouses.append({
                "WarehouseCD": f"WH_{branch_cd}_{counter:03}",
                "Description": random.choice(descriptions),
                "BranchCD": branch_cd,
                "Active": True
            })
            counter += 1

        if counter > 200:
            break

    return pd.DataFrame(warehouses)
