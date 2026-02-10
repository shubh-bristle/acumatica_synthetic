
import random
import string
import pandas as pd
import pycountry

from acumatica import NUM_CURRENCIES, NUM_WAREHOUSES, PREFERRED_CURRENCIES


def currency():
    """
    Generates real-world ISO 4217 currencies
    ensuring all preferred currencies are included.
    """

    currencies = []
    seen = set()

    all_currencies = list(pycountry.currencies)
    random.shuffle(all_currencies)

    # ---- STEP 1: Add preferred currencies FIRST ----
    for curr in all_currencies:
        if curr.alpha_3 in PREFERRED_CURRENCIES and curr.alpha_3 not in seen:
            currencies.append({
                "CurrencyID": curr.alpha_3,
                "Description": curr.name,
                "DecimalPlaces": 0 if curr.alpha_3 in {"JPY", "KRW"} else 2,
                "Active": True
            })
            seen.add(curr.alpha_3)

    # ---- STEP 2: Fill remaining slots ----
    for curr in all_currencies:
        if len(currencies) >= NUM_CURRENCIES:
            break

        if not hasattr(curr, "alpha_3"):
            continue

        code = curr.alpha_3

        if code in seen:
            continue

        currencies.append({
            "CurrencyID": code,
            "Description": curr.name,
            "DecimalPlaces": 0 if code in {"JPY", "KRW"} else 2,
            "Active": True
        })
        seen.add(code)

    return pd.DataFrame(currencies)

def ledger(currency_df):
  
    ledger_blueprint = [
        ("ACTUAL",        "Actual"),
        ("BUDGET",        "Budget"),
        ("BUDGET_OPS",    "Budget"),
        ("BUDGET_CAPEX",  "Budget"),
        ("STAT_QTY",      "Statistical"),
        ("STAT_HOURS",    "Statistical"),
        ("REPORT_MGMT",   "Report"),
        ("REPORT_CONSOL", "Report")
    ]

    ledger_context_map = {
        "ACTUAL": "day-to-day financial activity across the company",
        "OPS": "department operating expenses",
        "CAPEX": "long-term asset investments",
        "QTY": "inventory movement and production volumes",
        "HOURS": "employee effort and labor tracking",
        "MGMT": "internal management review",
        "CONSOL": "group-level financial rollups",
        "DEFAULT": "general business planning"
    }

    # Natural, varied, non-repeating phrasing
    description_templates = {
        "Actual": [
            "This is the main book where real financial transactions are posted.",
            "All finalized accounting entries flow through this core ledger.",
            "This ledger reflects the company’s actual financial performance."
        ],
        "Budget": [
            "Holds planned numbers used during financial planning cycles.",
            "Used by finance teams to map out expected spending and revenue.",
            "Captures budget figures that guide business planning."
        ],
        "Statistical": [
            "Tracks operational metrics like counts, units, or hours.",
            "Used for measuring activity levels rather than money.",
            "Records non-financial data tied to operational performance."
        ],
        "Report": [
            "Designed for reporting and analysis rather than daily posting.",
            "Used to organize balances for leadership and financial review.",
            "Supports summary-level reporting across the organization."
        ]
    }

    used_descriptions = set()
    ledgers = []

    preferred_currency_df = currency_df[
        currency_df["CurrencyID"].isin(["USD", "EUR", "CAD"])
    ]

    for ledger_cd, balance_type in ledger_blueprint:

        # Determine context naturally
        context = ledger_context_map["DEFAULT"]
        for key in ledger_context_map:
            if key in ledger_cd:
                context = ledger_context_map[key]
                break

        # Pick a truly unique description
        while True:
            base_desc = random.choice(description_templates[balance_type])

            if balance_type == "Actual":
                description = f"{base_desc} It supports {context}."
            elif balance_type == "Budget":
                description = f"{base_desc} It plays a key role in {context}."
            elif balance_type == "Statistical":
                description = f"{base_desc} Commonly used for {context}."
            else:  # Report
                description = f"{base_desc} Primarily tied to {context}."

            if description not in used_descriptions:
                used_descriptions.add(description)
                break

        currency_id = (
            random.choice(preferred_currency_df["CurrencyID"].tolist())
            if balance_type in ["Actual", "Budget"]
            else None
        )

        ledgers.append({
            "LedgerCD": ledger_cd,
            "Description": description,
            "BalanceType": balance_type,
            "CurrencyID": currency_id,
            "Active": True
        })

    return pd.DataFrame(ledgers)

def branch(ledger_df):
    """
    Generates realistic Acumatica branches aligned to:
    - Single shared ACTUAL ledger
    - Base currency derived from branch region
    - Least operationally critical branch marked inactive (~10%)
    """

    # Ensure ACTUAL ledger exists
    if "ACTUAL" not in ledger_df["LedgerCD"].values:
        raise ValueError("Ledger 'ACTUAL' must exist in ledger table.")

    branch_master = {
        "HQ-NY": ("Corporate HQ - New York", "Corporate", "North America"),
        "MFG-TX": ("Texas Manufacturing Plant", "Manufacturing", "North America"),
        "DIST-CA": ("California Distribution Center", "Distribution", "North America"),
        "SRV-FL": ("Florida Service Center", "Service", "North America"),
        "OPS-TOR": ("Toronto Operations Center", "Operations", "Canada"),
        "SALE-LDN": ("London Sales Office", "Sales", "United Kingdom"),
        "EU-AMS": ("Amsterdam Logistics Hub", "Logistics", "Europe"),
        "IN-BLR": ("Bangalore Delivery Center", "Delivery", "India"),
        "AUS-SYD": ("Sydney Regional Office", "Regional Office", "Australia"),
        "RETAIL-NV": ("Las Vegas Flagship Store", "Retail", "North America")
    }

    region_currency_map = {
        "North America": "USD",
        "Canada": "CAD",
        "United Kingdom": "GBP",
        "Europe": "EUR",
        "India": "INR",
        "Australia": "AUD"
    }

    verbs = ["supports", "runs", "oversees", "manages", "coordinates"]
    used_descriptions = set()
    branches = []

    for branch_cd, (branch_name, branch_type, region) in branch_master.items():

        base_currency = region_currency_map.get(region, "USD")
        ledger_cd = "ACTUAL" ## Single posting ledger for all operational branches(Manufacturing, Distribution, Sales, Service)

        desc_style = random.choices(
            ["short", "medium", "long"],
            weights=[0.4, 0.35, 0.25]
        )[0]

        region_text = region.lower()
        branch_type_text = branch_type.lower()

        if desc_style == "short":
            description = f"{branch_name} handling {branch_type_text} operations in {region_text}"

        elif desc_style == "medium":
            description = (
                f"{branch_name} {random.choice(verbs)} day-to-day {branch_type_text} work across {region_text}"
            )

        else:
            description = (
                f"{branch_name} {random.choice(verbs)} core {branch_type_text} functions in {region_text}, "
                f"with financial activity recorded in the corporate actual ledger"
            )

        if description in used_descriptions:
            description += f" ({branch_type_text})"
        used_descriptions.add(description)

        branches.append({
            "BranchCD": branch_cd,
            "BranchName": branch_name,
            "BranchType": branch_type,
            "Region": region,
            "BaseCurrency": base_currency,
            "LedgerCD": ledger_cd,
            "Description": description,
            "Active": True
        })

    # Mark least operationally critical branch inactive
    inactivity_priority = ["Retail", "Sales", "Regional Office"]

    for priority in inactivity_priority:
        candidates = [b for b in branches if b["BranchType"] == priority]
        if candidates:
            candidates[0]["Active"] = False
            break

    return pd.DataFrame(branches)

def warehouse(branch_df):
    """
    Generates exactly 20 Acumatica-style warehouses with:
    - Human-like varied descriptions (short/medium/long)
    - StorageCategory aligned with CapacityClass & WarehouseType
    - ERP-safe branch linkage
    """

    branch_df = branch_df[branch_df["Active"] == True].reset_index(drop=True)
    if branch_df.empty:
        raise ValueError("No active branches available.")

    warehouses = []
    used_codes = set()

    # -------------------------------------------------
    # STORAGE CATEGORY MASTER (No N/A)
    # -------------------------------------------------
    storage_profiles = {
        "Ambient":       {"capacity": ["Medium", "Large"], "type": ["Distribution"]},
        "Industrial":    {"capacity": ["Large"], "type": ["Distribution"]},
        "Climate-Cntrl": {"capacity": ["Medium", "Large"], "type": ["Fulfillment"]},
        "Restricted":    {"capacity": ["Small", "Medium"], "type": ["Fulfillment"]},
        "Secure":        {"capacity": ["Small", "Medium"], "type": ["Fulfillment"]},
        "Mobile":        {"capacity": ["Small"], "type": ["Transit"]},
        "Off-site":      {"capacity": ["Medium", "Large"], "type": ["Distribution"]},
        "Waste":         {"capacity": ["Small"], "type": ["Transit"]},
        "Returns":       {"capacity": ["Medium"], "type": ["Distribution"]}
    }

    # -------------------------------------------------
    # Warehouse purpose naming aligned to storage type
    # -------------------------------------------------
    storage_name_map = {
        "Ambient": "General Storage",
        "Industrial": "Industrial Storage",
        "Climate-Cntrl": "Climate Controlled Storage",
        "Restricted": "Restricted Access Storage",
        "Secure": "Secure Storage",
        "Mobile": "Mobile Storage Unit",
        "Off-site": "Off-site Storage",
        "Waste": "Waste Handling Area",
        "Returns": "Returns Processing Center"
    }

    # -------------------------------------------------
    # Description Builder (Aligned to branch + purpose)
    # -------------------------------------------------
    def build_description(storage, branch_name, region):
        purpose = storage_name_map[storage]

        short_templates = [
            f"{purpose} supporting {branch_name}.",
            f"{purpose} facility in the {region.lower()} region.",
            f"{purpose} location for branch logistics."
        ]

        medium_templates = [
            f"This {purpose.lower()} supports inventory operations for {branch_name} in the {region.lower()} region.",
            f"Facility dedicated to {purpose.lower()} activities aligned with {branch_name}.",
            f"Handles {purpose.lower()} needs while supporting logistics across the {region.lower()} area."
        ]

        long_templates = [
            f"This facility provides {purpose.lower()} services supporting inventory management and logistics operations for {branch_name} across the {region.lower()} region.",
            f"This warehouse is dedicated to {purpose.lower()}, helping ensure efficient material flow and storage reliability for {branch_name}.",
            f"Serving as a key {purpose.lower()} site, this location supports supply chain continuity and operational efficiency for {branch_name}."
        ]

        desc_type = random.choices(
            ["short", "medium", "long"],
            weights=[0.3, 0.45, 0.25]
        )[0]

        if desc_type == "short":
            return random.choice(short_templates)
        elif desc_type == "medium":
            return random.choice(medium_templates)
        else:
            return random.choice(long_templates)

    # -------------------------------------------------
    # Warehouse Code Generator (Aligned to function)
    # -------------------------------------------------
    def generate_warehouse_cd(branch_cd, storage):
        tag = storage[:3].upper()
        while True:
            suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=2))
            code = f"WH-{branch_cd}-{tag}{suffix}"
            if code not in used_codes:
                used_codes.add(code)
                return code

    # -------------------------------------------------
    # MAIN GENERATION LOOP
    # -------------------------------------------------
    while len(warehouses) < NUM_WAREHOUSES:
        branch = branch_df.sample(1).iloc[0]
        branch_cd = branch["BranchCD"]
        branch_name = branch["BranchName"]
        region = branch["Region"]

        storage = random.choice(list(storage_profiles.keys()))
        capacity = random.choice(storage_profiles[storage]["capacity"])
        wh_type = random.choice(storage_profiles[storage]["type"])

        warehouses.append({
            "WarehouseCD": generate_warehouse_cd(branch_cd, storage),
            "WarehouseName": f"{branch_name} {storage_name_map[storage]}",
            "WarehouseType": wh_type,
            "StorageCategory": storage,
            "CapacityClass": capacity,
            "Description": build_description(storage, branch_name, region),
            "BranchCD": branch_cd,
            "Active": True
        })

    return pd.DataFrame(warehouses)