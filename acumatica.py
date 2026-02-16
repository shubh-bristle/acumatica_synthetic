from math import ceil
import string
import uuid
from faker import Faker
import pandas as pd
import random
import os
from datetime import datetime, timedelta
import pycountry

fake = Faker()

# ================= CONFIG =================

NUM_CURRENCIES = 50   #MasterData Increased quantity
NUM_LEDGERS = 80      #MasterData Increased quantity
NUM_BRANCHES = 36    #MasterData Increased quantity
NUM_WAREHOUSES = 200  #MasterData Increased quantity

NUM_EMPLOYEES = 300
NUM_PROJECTS = 200

NUM_ACCOUNTS = 250
NUM_SUBACCOUNTS = 150

NUM_ITEMCLASSES = 10
NUM_STOCKITEMS = 5000
NUM_NONSTOCK_ITEMS = 1000

NUM_CUSTOMER_CLASSES = 10
NUM_CUSTOMERS = 5000

NUM_VENDORS = 2000

NUM_SALES_ORDERS = 150000
NUM_PURCHASE_ORDERS = 60000
NUM_JOURNAL_TRANSACTIONS = 300000
NUM_TERMS = 6
NUM_NUMBERING = 6
NUM_UOM = 8
NUM_BILLS =60000

OUTPUT_DIR = "acumatica_1M_Data"

# ================= DEDUPLICATION HELPER =================Write explanation

def deduplicate(df, keys, entity_name):
    before = len(df)
    df = df.drop_duplicates(subset=keys, keep="first")
    removed = before - len(df)
    if removed > 0:
        print(f"⚠ Removed {removed} duplicate rows from {entity_name}")
    return df

#============================================================================
PREFERRED_CURRENCIES = {
    "USD", "GBP", "INR", "AED", "CAD",
    "SAR", "AUD", "PKR", "BRL", "EGP"
}

def build_currency_weights(currency_df):
    currency_ids = currency_df["CurrencyID"].tolist()
    weights = [
        5 if cid in PREFERRED_CURRENCIES else 1
        for cid in currency_ids
    ]
    return currency_ids, weights


# ================= MASTER DATA =================


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
    """
    Dynamically generates Acumatica ledgers with realistic,
    production-style descriptions and valid ledger types.
    """

    ledger_type_plan = (
        ["Actual"] * 30 +
        ["Budget"] * 30 +
        ["Statistical"] * 10 +
        ["Report"] * 10
    )

    ledger_type_plan = ledger_type_plan[:NUM_LEDGERS]

    type_counter = {
        "Actual": 1,
        "Budget": 1,
        "Statistical": 1,
        "Report": 1
    }

    description_templates = {
        "Actual": [
            "Primary ledger used to record actual financial transactions.",
            "Secondary actual ledger used for additional financial tracking.",
            "Additional actual ledger used for parallel accounting purposes."
        ],
        "Budget": [
            "Budget ledger used for financial planning and budget control.",
            "Secondary budget ledger used for alternative budget scenarios.",
            "Additional budget ledger used for forecasting and analysis."
        ],
        "Statistical": [
            "Statistical ledger used to record non financial data for reporting."
        ],
        "Report": [
            "Reporting ledger used for financial analysis and consolidated reporting."
        ]
    }

    ledgers = []

    for balance_type in ledger_type_plan:
        idx = type_counter[balance_type]
        type_counter[balance_type] += 1

        ledger_cd = f"{balance_type[:3].upper()}_{idx:02}"

        descriptions = description_templates[balance_type]
        description = (
            descriptions[idx - 1]
            if idx - 1 < len(descriptions)
            else f"{balance_type} ledger used for financial reporting."
        )

        ledgers.append({
            "LedgerCD": ledger_cd,
            "Description": description,
            "BalanceType": balance_type,
            "CurrencyID": (
                build_currency_weights(currency_df)
                if balance_type in ["Actual", "Budget"]
                else None
            ),
            "Active": True
        })

    return pd.DataFrame(ledgers)


def branches(ledger_df):
    CORPORATE_ACTIONS = [
    "directs executive leadership",
    "oversees corporate governance",
    "drives strategic planning initiatives",
    "manages enterprise financial oversight"
    ]

    MANUFACTURING_ACTIONS = [
        "oversees large-scale production",
        "manages industrial fabrication processes",
        "coordinates assembly operations",
        "leads quality-controlled manufacturing cycles"
    ]

    DISTRIBUTION_ACTIONS = [
        "manages high-volume warehousing",
        "coordinates inventory allocation",
        "optimizes outbound fulfillment workflows",
        "oversees regional stock distribution"
    ]

    SERVICE_ACTIONS = [
        "delivers technical support services",
        "provides field maintenance operations",
        "coordinates customer assistance programs",
        "manages service-level performance delivery"
    ]

    OPERATIONS_ACTIONS = [
        "coordinates cross-functional business processes",
        "monitors operational performance metrics",
        "drives efficiency improvement initiatives",
        "oversees process optimization programs"
    ]

    LOGISTICS_ACTIONS = [
        "manages transportation planning",
        "coordinates freight movement operations",
        "oversees carrier network partnerships",
        "controls inbound and outbound shipment execution"
    ]

    SCOPE_PHRASES = [
        "supporting domestic market demand",
        "serving regional commercial operations",
        "across key metropolitan areas",
        "within its strategic geographic footprint",
        "supporting multi-state distribution networks"
    ]

    # 🔥 Validate Ledger Relationship
    valid_ledgers = ledger_df[
        (ledger_df["BalanceType"] == "Actual") &
        (ledger_df["Active"] == True)
    ]["LedgerCD"].tolist()

    if not valid_ledgers:
        raise ValueError("No valid Actual ledgers found.")

    BRANCH_TYPE_MAP = {
        "HQ": "Corporate",
        "MFG": "Manufacturing",
        "DIST": "Distribution",
        "SRV": "Service",
        "OPS": "Operations",
        "LOG": "Logistics"
    }

    LOCATION_MAP = {
        "NY": ("New York", "North America"),
        "TX": ("Texas", "North America"),
        "CA": ("California", "North America"),
        "FL": ("Florida", "North America"),
        "CHI": ("Chicago", "North America"),
        "NJ": ("New Jersey", "North America")
    }

    BASE_CURRENCY_MAP = {
        "North America": "USD"
    }

    def generate_description(branch_type, branch_name, region):

        action_map = {
            "Corporate": CORPORATE_ACTIONS,
            "Manufacturing": MANUFACTURING_ACTIONS,
            "Distribution": DISTRIBUTION_ACTIONS,
            "Service": SERVICE_ACTIONS,
            "Operations": OPERATIONS_ACTIONS,
            "Logistics": LOGISTICS_ACTIONS
        }

        action = random.choice(action_map[branch_type])
        scope = random.choice(SCOPE_PHRASES)

        description = (
            f"{branch_name} {action}, {scope} across the {region} region."
        )

        return description


    branch_rows = []
    used_codes = set()

    type_keys = list(BRANCH_TYPE_MAP.keys())
    location_keys = list(LOCATION_MAP.keys())

    while len(branch_rows) < NUM_BRANCHES:

        type_code = random.choice(type_keys)
        loc_code = random.choice(location_keys)

        branch_cd = f"{type_code}-{loc_code}"

        if branch_cd in used_codes:
            continue

        used_codes.add(branch_cd)

        branch_type = BRANCH_TYPE_MAP[type_code]
        location_name, region = LOCATION_MAP[loc_code]
        base_currency = BASE_CURRENCY_MAP[region]

        branch_name = f"{branch_type} - {location_name}"

        description = generate_description(branch_type, branch_name, region)

        branch_rows.append({
            "BranchCD": branch_cd,
            "BranchName": branch_name,
            "BranchType": branch_type,
            "Region": region,
            "BaseCurrency": base_currency,
            "LedgerCD": random.choice(valid_ledgers),  # 🔥 Now relational
            "Description": description,
            "Active": True
        })

    df = pd.DataFrame(branch_rows)

    # 🔥 10% inactive
    inactive_count = ceil(NUM_BRANCHES * 0.10)
    inactive_indices = random.sample(list(df.index), inactive_count)
    df.loc[inactive_indices, "Active"] = False

    return df

def warehouse(branch_df):
    """
    Generates warehouse records with business rules applied:
    1. HQ branches cannot have Industrial / Secure / Waste warehouses
    2. Only ONE Secure Storage warehouse per branch
    """

    warehouses = []
    warehouse_counter = 1

    # Track Secure warehouses per branch
    secure_seen = set()

    for _, branch in branch_df.iterrows():
        branch_cd = branch["BranchCD"]

        # Detect HQ
        is_hq = branch_cd.startswith("HQ")

        num_warehouses = random.randint(1, 3)

        for _ in range(num_warehouses):
            if warehouse_counter > 210:
                break

            # --- Storage category selection ---
            storage_category = random.choice([
                "Ambient",
                "Restricted",
                "Climate-Control",
                "Industrial",
                "Waste",
                "Secure",
                "Off-site"
            ])

            # -------------------------------
            # RULE 1: HQ STORAGE RESTRICTION
            # -------------------------------
            if is_hq and storage_category in {"Industrial", "Secure", "Waste", "Off-site"}:
                continue  # skip invalid HQ warehouse

            # ------------------------------------
            # RULE 2: SECURE STORAGE DEDUPLICATION
            # ------------------------------------
            if storage_category == "Secure":
                if branch_cd in secure_seen:
                    continue
                secure_seen.add(branch_cd)

            warehouses.append({
                "WarehouseCD": f"WH-{branch_cd}-{warehouse_counter:04}",
                "WarehouseName": f"{branch_cd} {storage_category} Storage",
                "WarehouseType": (
                    "Fulfillment" if storage_category in {"Secure", "Restricted", "Climate-Cntrl"}
                    else "Distribution"
                ),
                "StorageCategory": storage_category,
                "CapacityClass": random.choice(["Small", "Medium", "Large"]),
                "Description": f"{storage_category} storage location for branch logistics.",
                "BranchCD": branch_cd,
                "Active": True
            })

            warehouse_counter += 1

        if warehouse_counter > 210:
            break

    return pd.DataFrame(warehouses)


def accounts():
    """
    Generates realistic Acumatica GL accounts with UUID4 AccountCD
    and production grade descriptions without special characters.
    """
    account_type_map = {
        "Asset": [
            (
                "Cash Operating Account",
                "Primary operating bank account used for daily cash receipts and disbursements including customer payments and vendor settlements."
            ),
            (
                "Accounts Receivable",
                "Outstanding customer invoices representing amounts due for goods delivered and services rendered."
            ),
            (
                "Inventory",
                "Carrying value of inventory on hand including raw materials work in process and finished goods held for sale."
            ),
            (
                "Prepaid Expenses",
                "Payments made in advance for services or benefits to be recognized as expenses over future accounting periods."
            ),
            (
                "Fixed Assets",
                "Capitalized cost of tangible long term assets such as buildings machinery vehicles and equipment."
            )
        ],
        "Liability": [
            (
                "Accounts Payable",
                "Amounts owed to suppliers and vendors for goods and services received but not yet paid."
            ),
            (
                "Accrued Expenses",
                "Expenses that have been incurred but not yet invoiced or paid as of the reporting date."
            ),
            (
                "Loans and Notes Payable",
                "Outstanding balances on short term and long term borrowing arrangements including principal obligations."
            ),
            (
                "Payroll Liabilities",
                "Employee related liabilities including wages payable payroll taxes and statutory deductions."
            )
        ],
        "Income": [
            (
                "Product Sales Revenue",
                "Revenue earned from the sale of manufactured or purchased goods to customers."
            ),
            (
                "Service Revenue",
                "Revenue earned from professional services maintenance contracts and billable labor."
            ),
            (
                "Interest Income",
                "Income earned from interest bearing accounts deposits or financing arrangements."
            ),
            (
                "Other Operating Income",
                "Non core operating income not directly related to primary business activities."
            )
        ],
        "Expense": [
            (
                "Cost of Goods Sold",
                "Direct costs attributable to the production or purchase of goods sold during the accounting period."
            ),
            (
                "Rent and Facility Expense",
                "Costs associated with leasing office space warehouses manufacturing facilities and related property expenses."
            ),
            (
                "Utilities Expense",
                "Costs for electricity water gas telecommunications and other utility services."
            ),
            (
                "Salaries and Wages",
                "Compensation expense for employees including regular wages overtime and bonuses."
            ),
            (
                "Depreciation Expense",
                "Periodic allocation of the cost of capitalized assets over their estimated useful lives."
            )
        ]
    }

    accounts_list = []
    types = list(account_type_map.keys())

    for _ in range(NUM_ACCOUNTS):
        acc_type = random.choice(types)
        name, desc = random.choice(account_type_map[acc_type])

        accounts_list.append({
            "AccountCD": str(uuid.uuid4()),
            "Description": f"{name} {desc}",
            "Type": acc_type,
            "Active": True
        })

    return pd.DataFrame(accounts_list)

def subaccounts():
    """
    Generates realistic and user-friendly SubAccounts with UUID4 SubAccountCDs.
    SubAccounts provide detailed financial tracking for departments, regions, projects, and activities.
    """

    # Realistic, descriptive SubAccount names
    subaccount_descriptions = [
        "North Region Operations",
        "South Region Operations",
        "East Warehouse",
        "West Distribution Center",
        "Marketing Department",
        "Sales Department",
        "Research & Development",
        "IT Department",
        "Human Resources",
        "Finance Department",
        "Project Alpha Costs",
        "Project Beta Implementation",
        "Customer Onboarding Project",
        "Training Expenses",
        "Maintenance & Repairs",
        "Travel & Entertainment",
        "Office Supplies",
        "Consulting Services",
        "Quality Assurance",
        "Miscellaneous Expenses"
    ]

    # Cycle descriptions if NUM_SUBACCOUNTS > len(subaccount_descriptions)
    accounts_list = []
    for i in range(NUM_SUBACCOUNTS):
        desc = subaccount_descriptions[i % len(subaccount_descriptions)]
        accounts_list.append({
            "SubAccountCD": str(uuid.uuid4()),  # Unique identifier
            "Description": desc,               # Realistic, user-friendly description
            "Active": True
        })

    return pd.DataFrame(accounts_list)

def itemclass(accounts_df, subaccounts_df):
    """
    Generates realistic ItemClass master data with UUID4 ItemClassCD
    and production ready descriptions without special characters.

    Interrelationship:
    Account.AccountCD        ← ItemClass.InventoryAccountCD
    SubAccount.SubAccountCD  ← ItemClass.InventorySubAccountCD
    """

    # ERP realistic inventory classifications
    categories = [
        "Finished Goods",
        "Raw Materials",
        "Production Consumables",
        "Packaging Materials",
        "Spare Parts",
        "Office Supplies",
        "Tools and Equipment",
        "Chemical Materials",
        "Electronic Components",
        "Safety Supplies"
    ]

    descriptors = [
        "Manufactured Items for Sale",
        "Purchased Components Used in Production",
        "Materials Consumed During Manufacturing",
        "Items Used for Packing and Shipping",
        "Replacement Parts for Maintenance",
        "Items Used for Administrative Operations",
        "Operational Tools Used in Daily Activities",
        "Industrial Chemicals Used in Production",
        "Electrical and Electronic Components",
        "Protective Equipment for Workplace Safety"
    ]

    item_classes = []

    for _ in range(NUM_ITEMCLASSES):
        itemclass_cd = str(uuid.uuid4())

        category = random.choice(categories)
        descriptor = random.choice(descriptors)

        # Final description is human readable and ERP friendly
        description = f"{category} {descriptor}"

        inventory_account_cd = accounts_df.sample(1)["AccountCD"].values[0]
        inventory_subaccount_cd = subaccounts_df.sample(1)["SubAccountCD"].values[0]

        item_classes.append({
            "ItemClassCD": itemclass_cd,
            "Description": description,
            "InventoryAccountCD": inventory_account_cd,
            "InventorySubAccountCD": inventory_subaccount_cd,
            "Active": True
        })

    return pd.DataFrame(item_classes)

def stockitem(itemclass_df, uom_df):
    """    
    Interrelationships:
    ItemClass.ItemClassCD ← StockItem.ItemClassCD
    UnitOfMeasure.UOM     ← StockItem.BaseUnit
    InventoryAccountCD and InventorySubAccountCD come from the linked ItemClass
    """

    stock_items = []

    # Realistic inventory item descriptors used in ERP systems
    item_descriptors = [
        "Manufactured Component",
        "Finished Product",
        "Replacement Part",
        "Production Material",
        "Maintenance Item",
        "Packaging Item",
        "Electrical Component",
        "Mechanical Component",
        "Safety Supply",
        "Industrial Supply"
    ]

    for _ in range(NUM_STOCKITEMS):
        item_class_row = itemclass_df.sample(1).iloc[0]
        base_unit = uom_df.sample(1).iloc[0]["UOM"]

        random_letters = ''.join(random.choices(string.ascii_uppercase, k=4))
        inventory_cd = f"STK{random_letters}{random.randint(100,999)}"

        # ERP realistic clean description
        description = f"{item_class_row['Description']} {random.choice(item_descriptors)}"

        stock_items.append({
            "InventoryCD": inventory_cd,
            "Description": description,
            "ItemClassCD": item_class_row["ItemClassCD"],
            "BaseUnit": base_unit,
            "InventoryAccountCD": item_class_row["InventoryAccountCD"],
            "InventorySubAccountCD": item_class_row["InventorySubAccountCD"],
            "Active": True
        })

    return pd.DataFrame(stock_items)

def nonstock_items(itemclass_df, uom_df):
    """
    Generates realistic NonStockItem master data with mandatory fields.
    Interrelationships:
      - ItemClass.ItemClassCD  ← NonStockItem.ItemClassCD
      - UnitOfMeasure.UOM      ← NonStockItem.BaseUnit
      - ExpenseAccountCD and ExpenseSubAccountCD come from linked ItemClass
    """

    nonstock_items_list = []

    for _ in range(NUM_NONSTOCK_ITEMS):
        item_class_row = itemclass_df.sample(1).iloc[0]
        base_unit = uom_df.sample(1).iloc[0]["UOM"]

        random_letters = ''.join(random.choices(string.ascii_uppercase, k=4))
        inventory_cd = f"NS-{random_letters}-{random.randint(100,999)}"

        # ERP realistic non stock item descriptions without special characters
        descriptor = random.choice([
            "Consulting Service",
            "Maintenance Service",
            "Employee Training",
            "Installation Service",
            "Technical Support Service",
            "Software License Fee",
            "Software Subscription Service",
            "Transportation Service",
            "Contract Labor Service",
            "Utility Service Expense"
        ])

        description = f"{item_class_row['Description']} {descriptor}"

        nonstock_items_list.append({
            "InventoryCD": inventory_cd,
            "Description": description,
            "ItemClassCD": item_class_row["ItemClassCD"],
            "BaseUnit": base_unit,
            "ExpenseAccountCD": item_class_row["InventoryAccountCD"],
            "ExpenseSubAccountCD": item_class_row["InventorySubAccountCD"],
            "Active": True
        })

    return pd.DataFrame(nonstock_items_list)

def customer_class(accounts_df, subaccounts_df, terms_df):
    """
    Generates realistic CustomerClass master data with interrelationships:
      - Account.AccountCD       ← CustomerClass.ARAccountCD
      - SubAccount.SubAccountCD ← CustomerClass.ARSubAccountCD
      - Terms.TermsID           ← CustomerClass.TermsID
      
    Fields:
      CustomerClassID, Description, ARAccountCD, ARSubAccountCD, TermsID, Active
    """
    
    class_types = [
        "Retail", "Wholesale", "Corporate", "Government",
        "SME", "Distributor", "Online", "International",
        "VIP", "Partner"
    ]
    
    business_focus = [
        "high volume orders",
        "long term contracts",
        "seasonal purchases",
        "frequent repeat transactions",
        "strategic partnership",
        "priority support",
        "custom solutions",
        "regional distribution"
    ]
    
    customer_classes = []
    
    for i in range(NUM_CUSTOMER_CLASSES):
        customer_class_id = f"CC{i+1:02}"
        
        class_type = random.choice(class_types)
        focus = random.choice(business_focus)
        
        # ASCII safe realistic description
        description = f"{class_type} customers typically engaged in {focus}"
        
        ar_account_cd = random.choice(accounts_df["AccountCD"])
        ar_subaccount_cd = random.choice(subaccounts_df["SubAccountCD"])
        terms_id = random.choice(terms_df["TermsID"])
        
        customer_classes.append({
            "CustomerClassID": customer_class_id,
            "Description": description,
            "ARAccountCD": ar_account_cd,
            "ARSubAccountCD": ar_subaccount_cd,
            "TermsID": terms_id,
            "Active": True
        })
    
    return pd.DataFrame(customer_classes)

def customers(customer_class_df, terms_df):
    """
    Generates realistic Customer master data with interrelationships:
      - CustomerClass.CustomerClassID ← Customer.CustomerClassID
      - Terms.TermsID                  ← Customer.TermsID

    Mandatory fields:
      CustomerCD, CustomerName, CustomerClassID, TermsID, Active
    """

    customers_list = []

    for i in range(NUM_CUSTOMERS):
        # Generate unique CustomerCD like CUST000001
        customer_cd = f"CUST{i+1:06}"

        # Generate realistic company name using Faker
        customer_name = fake.company()

        # Pick a random CustomerClass
        customer_class_row = customer_class_df.sample(1).iloc[0]
        customer_class_id = customer_class_row["CustomerClassID"]

        # Pick a random TermsID from the Terms table
        terms_id = random.choice(terms_df["TermsID"])

        customers_list.append({
            "CustomerCD": customer_cd,
            "CustomerName": customer_name,
            "CustomerClassID": customer_class_id,
            "TermsID": terms_id,
            "Active": True
        })

    return pd.DataFrame(customers_list)

def vendors(terms_df):
    """
    Generates Acumatica-safe Vendor master data.

    Output columns:
      VendorCD (unique)
      VendorName
      VendorClassID
      TermsID
      CurrencyID
      Active

    Interrelationship:
      Terms.TermsID ← Vendor.TermsID
    """

    vendors_list = []

    # ---- CONFIG (internal, no change to function call) ----
    VENDOR_CLASSES = [
        "RAW_MATERIAL",
        "SERVICES",
        "UTILITIES",
        "LOGISTICS",
        "CAPEX"
    ]

    ALLOWED_CURRENCIES = ["USD", "EUR", "GBP", "INR", "AED"]

    for i in range(NUM_VENDORS):
        # Autonumbered, collision-proof
        vendor_cd = f"VND{i+1:06}"

        vendor_name = fake.company()

        # Bias toward credit terms (NET30-like)
        terms_id = random.choices(
            terms_df["TermsID"],
            weights=[
                25 if t.upper() in ("NET30", "NET45", "NET60") else
                15 if t.upper() in ("2P10N30", "NET7") else
                5   # PREPAID / COD / IMMEDIATE
                for t in terms_df["TermsID"]
            ]
        )[0]

        vendor_class_id = random.choice(VENDOR_CLASSES)

        currency_id = random.choice(ALLOWED_CURRENCIES)

        vendors_list.append({
            "VendorCD": vendor_cd,
            "VendorName": vendor_name,
            "VendorClassID": vendor_class_id,
            "TermsID": terms_id,
            "CurrencyID": currency_id,
            "Active": True
        })

    return pd.DataFrame(vendors_list)

def employees(branch_df):
    """
    Generates realistic Employee master data aligned with branch roles.

    Mandatory fields:
      EmployeeCD, EmployeeName, BranchCD, Active

    Interrelationship:
      Branch.BranchCD ← Employee.BranchCD
    """

    # Realistic employee distribution by branch
    branch_employee_distribution = {
        "HQ": 0.25,         # Management, finance, IT
        "US_EAST": 0.10,
        "US_WEST": 0.10,
        "EMEA": 0.10,
        "MFG01": 0.15,      # Manufacturing workforce
        "DC01": 0.10,       # Logistics
        "STORE01": 0.08,    # Retail staff
        "SERVICES": 0.06,   # Consulting / services
        "RND": 0.04,        # Engineers
        "SHARED": 0.02      # Back-office shared services
    }

    employees_list = []
    employee_counter = 1

    for _, branch in branch_df.iterrows():
        branch_cd = branch["BranchCD"]

        # Calculate employees for this branch
        branch_ratio = branch_employee_distribution.get(branch_cd, 0.02)
        branch_employee_count = max(1, int(NUM_EMPLOYEES * branch_ratio))

        for _ in range(branch_employee_count):
            employees_list.append({
                "EmployeeCD": f"EMP{employee_counter:06}",
                "EmployeeName": fake.name(),
                "BranchCD": branch_cd,
                "Active": True
            })
            employee_counter += 1

    # Trim or pad to exact NUM_EMPLOYEES
    employees_df = pd.DataFrame(employees_list)

    if len(employees_df) > NUM_EMPLOYEES:
        employees_df = employees_df.sample(NUM_EMPLOYEES).reset_index(drop=True)
    elif len(employees_df) < NUM_EMPLOYEES:
        for _ in range(NUM_EMPLOYEES - len(employees_df)):
            employees_df = pd.concat([
                employees_df,
                pd.DataFrame([{
                    "EmployeeCD": f"EMP{employee_counter:06}",
                    "EmployeeName": fake.name(),
                    "BranchCD": random.choice(branch_df["BranchCD"]),
                    "Active": True
                }])
            ], ignore_index=True)
            employee_counter += 1

    return employees_df


def projects(customers_df, branch_df):
    """
    Generates realistic Project master data aligned with Branch structure.

    Mandatory fields:
      ProjectCD, Description, CustomerCD, BranchCD, Active

    Interrelationships:
      Customer.CustomerCD ← Project.CustomerCD
      Branch.BranchCD     ← Project.BranchCD
    """

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
        "Data Analytics & Reporting Program",
        "Regulatory Compliance Project",
        "Service Delivery Transformation",
        "Research and Development Initiative"
    ]

    projects_list = []
    used_project_cds = set()

    def generate_project_cd(branch_cd):
        while True:
            suffix = ''.join(
                random.choices(string.ascii_uppercase + string.digits, k=4)
            )
            project_cd = f"PRJ-{branch_cd}-{suffix}"
            if project_cd not in used_project_cds:
                used_project_cds.add(project_cd)
                return project_cd

    for _ in range(NUM_PROJECTS):
        branch_cd = random.choice(branch_df["BranchCD"])
        project_cd = generate_project_cd(branch_cd)

        projects_list.append({
            "ProjectCD": project_cd,
            "Description": random.choice(project_descriptions),
            "CustomerCD": random.choice(customers_df["CustomerCD"]),
            "BranchCD": branch_cd,
            "Active": True
        })

    return pd.DataFrame(projects_list)

# ================= TRANSACTIONS =================

def sales_orders(customer_df, branch_df, inventory_df):

    header_rows = []
    line_rows = []

    customers = customer_df["CustomerCD"].tolist()
    branches = branch_df[branch_df["Active"] == True]["BranchCD"].tolist()
    inventory = inventory_df[inventory_df["Active"] == True]

    # 🔥 PERFORMANCE FIX (avoid .sample() in loop)
    inventory_rows = inventory.to_dict("records")

    ship_via_options = ["DHL", "FedEx", "UPS", "BlueDart"]
    order_types = ["SO", "IN", "CM"]

    start_date = datetime(2024, 1, 1)

    for i in range(NUM_SALES_ORDERS):

        branch_cd = random.choice(branches)
        order_type = random.choice(order_types)
        customer = random.choice(customers)

        order_date = start_date + timedelta(days=random.randint(0, 730))

        # ✅ Status Based On OrderType
        if order_type == "SO":
            status = random.choices(
                ["Open", "Backordered", "Completed", "Cancelled"],
                weights=[40, 25, 25, 10]
            )[0]

        elif order_type == "IN":
            status = random.choices(
                ["Open", "Released", "Voided"],
                weights=[30, 60, 10]
            )[0]

        elif order_type == "CM":
            status = random.choices(
                ["Open", "Released", "Closed"],
                weights=[40, 40, 20]
            )[0]

        # ✅ RequestedDate only for SO
        if order_type == "SO":
            requested_date = order_date + timedelta(days=random.randint(3, 14))
            requested_date_str = requested_date.strftime("%d-%m-%Y")
        else:
            requested_date_str = None

        order_nbr = f"{order_type}-{branch_cd}-{order_date.year}-{random.randint(1000,9999)}"

        # 🔥 Generate Lines (1–5 per order)
        num_lines = random.randint(1, 5)
        order_total = 0

        for line_nbr in range(1, num_lines + 1):

            # ✅ FAST inventory selection
            item = random.choice(inventory_rows)
            inventory_id = item["InventoryCD"]

            qty = random.randint(1, 20)
            unit_price = round(random.uniform(50, 500), 2)

            # ✅ Credit Memo logic
            if order_type == "CM":
                qty = -qty

            line_amount = round(qty * unit_price, 2)
            order_total += line_amount

            line_rows.append({
                "OrderNbr": order_nbr,
                "LineNbr": line_nbr,
                "InventoryID": inventory_id,
                "Qty": qty,
                "UnitPrice": unit_price,
                "LineAmount": line_amount
            })

        # ✅ Financial Logic
        tax_total = round(order_total * 0.08, 2) if order_type != "CM" else 0
        freight = round(random.uniform(10, 100), 2) if order_type == "SO" else 0
        grand_total = round(order_total + tax_total + freight, 2)

        if status == "Cancelled":
            tax_total = 0
            freight = 0
            grand_total = 0


        header_rows.append({
            "OrderNbr": order_nbr,
            "OrderType": order_type,
            "CustomerCD": customer,
            "OrderDate": order_date.strftime("%d-%m-%Y"),
            "BranchCD": branch_cd,
            "Status": status,
            "ShipVia": random.choice(ship_via_options),
            "RequestedDate": requested_date_str,
            "OrderTotal": round(order_total, 2),
            "TaxTotal": tax_total,
            "FreightAmount": freight,
            "GrandTotal": grand_total
        })

    sales_order_header_df = pd.DataFrame(header_rows)
    sales_order_line_df = pd.DataFrame(line_rows)

    return sales_order_header_df, sales_order_line_df


def shipments(so_header_df, so_line_df):

    shipment_headers = []
    shipment_lines = []

    def generate_shipment_nbr(order_nbr):
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        return f"SHP-{order_nbr}-{suffix}"

    so_line_grouped = so_line_df.groupby("OrderNbr")

    for _, so in so_header_df.iterrows():

        order_nbr = so["OrderNbr"]

        if order_nbr not in so_line_grouped.groups:
            continue

        order_lines = so_line_grouped.get_group(order_nbr)

        shipment_nbr = generate_shipment_nbr(order_nbr)

        shipment_headers.append({
            "ShipmentNbr": shipment_nbr,
            "OrderNbr": order_nbr,
            "BranchCD": so["BranchCD"],
            "ShipmentDate": so["OrderDate"],
            "Status": "Confirmed"
        })

        # 🔥 Generate Shipment LineNbr safely
        for line_nbr, (_, line) in enumerate(order_lines.iterrows(), start=1):

            shipment_lines.append({
                "ShipmentNbr": shipment_nbr,
                "LineNbr": line_nbr,  # ✅ NOW EXISTS
                "OrderNbr": order_nbr,
                "InventoryID": line["InventoryID"],
                "Qty": line["Qty"]
            })

    return (
        pd.DataFrame(shipment_headers),
        pd.DataFrame(shipment_lines)
    )



def sales_invoices(customers_df, branch_df):
    """
    Interrelationship:
      Customer.CustomerCD ← SalesInvoice.CustomerCD
    """

    invoices_list = []
    used_invoice_nbrs = set()

    def generate_invoice_nbr(branch_cd, invoice_date):
        while True:
            suffix = ''.join(
                random.choices(string.ascii_uppercase + string.digits, k=4)
            )
            invoice_nbr = f"INV-{branch_cd}-{invoice_date.year}-{suffix}"
            if invoice_nbr not in used_invoice_nbrs:
                used_invoice_nbrs.add(invoice_nbr)
                return invoice_nbr

    for _ in range(NUM_SALES_ORDERS):
        customer_cd = random.choice(customers_df["CustomerCD"])
        branch_cd = random.choice(branch_df["BranchCD"])
        invoice_date = fake.date_between(start_date="-6M", end_date="today")

        invoices_list.append({
            "InvoiceNbr": generate_invoice_nbr(branch_cd, invoice_date),
            "CustomerCD": customer_cd,
            "InvoiceDate": invoice_date,
            "BranchCD": branch_cd
        })

    return pd.DataFrame(invoices_list)

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
            "OrderType": random.choices(
                ["RO", "DP"],
                weights=[0.95, 0.05]
            )[0],
            "OrderNbr": gen_po(br, date),
            "VendorCD": vend,
            "Status": random.choice(["Open", "Released", "On Hold"]),
            "OrderDate": date,
            "ExpectedDate": date + timedelta(days=random.randint(5, 45)),
            "BranchCD": br,
            "TermsID": random.choice(["2P10N30", "NET7", "PREPAID", "EOM"]),
            "CurrencyCD": random.choice(["USD", "GBP", "EUR"]),
        })

    return pd.DataFrame(rows)


def purchase_order_lines(po_df, stk_df, nstk_df, wh_df, proj_df):
    rows = []

    items = pd.concat([stk_df, nstk_df], ignore_index=True)

    # Branch → Warehouses
    wh_map = (
        wh_df
        .groupby("BranchCD")["WarehouseCD"]
        .apply(list)
        .to_dict()
    )

    for _, po in po_df.iterrows():
        branch = po["BranchCD"]
        warehouses = wh_map.get(branch, [])

        if not warehouses:
            continue

        for line_nbr in range(1, random.randint(2, 6)):
            item = items.sample(1).iloc[0]

            rows.append({
                "OrderType": po["OrderType"],
                "OrderNbr": po["OrderNbr"],
                "LineNbr": line_nbr,
                "InventoryID": item["InventoryCD"],
                "OrderQty": random.randint(1, 50),
                "CuryUnitCost": round(random.uniform(10, 500), 2),
                "WarehouseCD": random.choice(warehouses),
                "ProjectID": random.choice(
                    proj_df["ProjectCD"].tolist() + [None]
                )
            })

    return pd.DataFrame(rows)


def purchase_receipts(purchase_orders_df, warehouse_df):
    """
    Generates realistic PurchaseReceipt header data (migration-safe).

    Key Fixes:
    - Warehouse belongs to same Branch as PO
    - Status skewed toward Open (migration-friendly)
    - Returns handled explicitly
    """

    receipts = []
    used_receipt_nbrs = set()

    # Pre-group warehouses by BranchCD (CRITICAL)
    wh_by_branch = (
        warehouse_df
        .groupby("BranchCD")["WarehouseCD"]
        .apply(list)
        .to_dict()
    )

    def generate_receipt_nbr(receipt_date):
        while True:
            suffix = ''.join(
                random.choices(string.ascii_uppercase + string.digits, k=4)
            )
            nbr = f"PR-{receipt_date.year}-{suffix}"
            if nbr not in used_receipt_nbrs:
                used_receipt_nbrs.add(nbr)
                return nbr

    for _, po in purchase_orders_df.iterrows():

        branch = po["BranchCD"]

        # ✅ Branch-safe warehouse selection
        warehouses = wh_by_branch.get(branch)
        if not warehouses:
            continue  # or raise, depending on strictness

        warehouse_cd = random.choice(warehouses)

        receipt_date = fake.date_between(
            start_date=po["OrderDate"],
            end_date="today"
        )

        # Receipt vs Return (small %)
        is_return = random.random() < 0.05

        receipt_type = "Return" if is_return else "Receipt"

        qty = round(random.uniform(1, 100), 2)
        amount = round(qty * random.uniform(20, 500), 2)

        if is_return:
            qty *= -1
            amount *= -1
            status = "Released"
        else:
            # Migration-safe status distribution
            status = random.choices(
                ["Open", "Released"],
                weights=[85, 15]
            )[0]

        receipts.append({
            "ReceiptNbr": generate_receipt_nbr(receipt_date),
            "OrderNbr": po["OrderNbr"],
            "VendorCD": po["VendorCD"],
            "WarehouseCD": warehouse_cd,
            "ReceiptDate": receipt_date,
            "ReceiptType": receipt_type,
            "Status": status,
            "ControlQty": qty,
            "TotalAmount": amount
        })

    return pd.DataFrame(receipts)


def bills(vendors_df, branch_df, receipts_df, purchase_orders_df):

    bills_list = []
    used_ref_nbrs = set()

    status_pool = (
        ["Balanced"] * 70 +
        ["Released"] * 15 +
        ["Closed"] * 10 +
        ["Hold"] * 5
    )

    # PO → Branch resolution (authoritative)
    po_branch_map = (
        purchase_orders_df
        .set_index("OrderNbr")["BranchCD"]
        .to_dict()
    )

    # ReceiptNbr → ReceiptDate lookup (for BillDate enforcement)
    receipt_date_map = (
        receipts_df
        .set_index("ReceiptNbr")["ReceiptDate"]
        .to_dict()
        if not receipts_df.empty
        else {}
    )
    receipt_rows = receipts_df.to_dict("records") if not receipts_df.empty else []


    def generate_ref_nbr(branch_cd, bill_date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"BILL-{branch_cd}-{bill_date.year}-{suf}"
            if nbr not in used_ref_nbrs:
                used_ref_nbrs.add(nbr)
                return nbr

    for _ in range(NUM_BILLS):

        vendor = vendors_df.sample(1).iloc[0]

        link_to_receipt = receipt_rows and random.random() < 0.65

        if link_to_receipt:
            receipt = random.choice(receipt_rows)
            receipt_nbr = receipt["ReceiptNbr"]


            # Branch must come from PO linked to receipt
            branch_cd = po_branch_map.get(
                receipt["OrderNbr"],
                random.choice(branch_df["BranchCD"])
            )

            # ✅ BillDate ≥ ReceiptDate
            receipt_date = pd.to_datetime(receipt_date_map[receipt_nbr])
            bill_date = fake.date_between(
                start_date=receipt_date,
                end_date=receipt_date + timedelta(days=90)
            )

        else:
            receipt_nbr = None
            branch_cd = random.choice(branch_df["BranchCD"])

            bill_date = fake.date_between(
                start_date="-24M",
                end_date="today"
            )

        # ✅ Always 2-decimal financial precision
        total_amount = round(random.uniform(500, 25000), 2)

        bills_list.append({
            "RefNbr": generate_ref_nbr(branch_cd, bill_date),
            "VendorCD": vendor["VendorCD"],
            "VendorRef": f"INV-{random.randint(100000, 999999)}",
            "BillDate": bill_date,
            "BranchCD": branch_cd,
            "CurrencyID": vendor["CurrencyID"],
            "ReceiptNbr": receipt_nbr,
            "TotalAmount": total_amount,
            "Status": random.choice(status_pool)
        })

    return pd.DataFrame(bills_list)


def journal_transactions(ledger_df, branch_df, account_df, subaccount_df, currency_df):

    rows = []

    actual_ledgers = ledger_df[
        ledger_df["BalanceType"] == "Actual"
    ]["LedgerCD"].tolist()

    branch_list = branch_df["BranchCD"].tolist()
    account_list = account_df["AccountCD"].tolist()
    subaccount_list = subaccount_df["SubAccountCD"].tolist()

    currency_ids, currency_weights = build_currency_weights(currency_df)

    # 🔥 Precompute date range ONCE
    today = datetime.today().date()
    start_date = today - timedelta(days=365)

    for i in range(NUM_JOURNAL_TRANSACTIONS):

        batch_nbr = f"JB{i+1:06}"
        amount = round(random.uniform(100, 5000), 2)

        currency = random.choices(
            currency_ids,
            weights=currency_weights,
            k=1
        )[0]

        txn_date = start_date + timedelta(
            days=random.randint(0, 365)
        )

        common_fields = {
            "BatchNbr": batch_nbr,
            "LedgerCD": random.choice(actual_ledgers),
            "BranchCD": random.choice(branch_list),
            "CurrencyID": currency,
            "TransactionDate": txn_date,
            "Active": True
        }

        rows.append({
            **common_fields,
            "AccountCD": random.choice(account_list),
            "SubAccountCD": random.choice(subaccount_list),
            "DebitAmt": amount,
            "CreditAmt": 0
        })

        rows.append({
            **common_fields,
            "AccountCD": random.choice(account_list),
            "SubAccountCD": random.choice(subaccount_list),
            "DebitAmt": 0,
            "CreditAmt": amount
        })

    return pd.DataFrame(rows)


def terms():
    rows = [
        # -------------------------------------------------
        # Immediate / Cash Terms
        # -------------------------------------------------
        {
            "TermsID": "COD",
            "Description": "Cash on Delivery",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 0,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },
        {
            "TermsID": "PREPAID",
            "Description": "Paid in advance before shipment",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 0,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },
        {
            "TermsID": "IMMEDIATE",
            "Description": "Payment due immediately upon invoice",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 0,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },

        # -------------------------------------------------
        # Standard Net Terms (Manufacturing / Logistics)
        # -------------------------------------------------
        {
            "TermsID": "NET7",
            "Description": "Net 7 days",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 7,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },
        {
            "TermsID": "NET30",
            "Description": "Net 30 days",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 30,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },
        {
            "TermsID": "NET45",
            "Description": "Net 45 days",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 45,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },
        {
            "TermsID": "NET60",
            "Description": "Net 60 days",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 60,
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        },

        # -------------------------------------------------
        # Discount Term (Fully Functional)
        # -------------------------------------------------
        {
            "TermsID": "2P10N30",
            "Description": "2% discount if paid within 10 days, otherwise Net 30",
            "DueDayType": "FixedNumberOfDays",
            "DueDays": 30,
            "DiscountDayType": "FixedNumberOfDays",
            "DiscountPercent": 2.00,
            "DiscountDays": 10,
            "Active": True
        },

        # -------------------------------------------------
        # End of Month (Correct Acumatica Logic)
        # -------------------------------------------------
        {
            "TermsID": "EOM",
            "Description": "Payment due at end of month",
            "DueDayType": "EndOfMonth",
            "DueDays": 0,  # MUST be 0 for EOM logic
            "DiscountDayType": None,
            "DiscountPercent": 0.0,
            "DiscountDays": 0,
            "Active": True
        }
    ]

    return pd.DataFrame(rows)

def numbering():
    """
    Generates Acumatica numbering sequences with descriptive business-friendly descriptions.
    """
    numbering_metadata = {
        "SO": "Automatically generates unique identifiers for Sales Order documents.",
        "PO": "Automatically generates unique identifiers for Purchase Order documents.",
        "INV": "Automatically generates unique identifiers for Accounts Receivable invoice documents.",
        "BILL": "Automatically generates unique identifiers for Accounts Payable vendor bill documents.",
        "GL": "Automatically generates unique batch numbers for General Ledger journal transactions.",
        "CUST": "Automatically generates unique account IDs for Customer master records."
    }

    return pd.DataFrame([{
        "NumberingID": f"{p}_SEQ",
        "Description": numbering_metadata[p],
        "NewSymbol": p,
        "StartNbr": "000001",
        "EndNbr": "999999",
        "LastNbr": "000000",
        "Active": True
    } for p in list(numbering_metadata.keys())[:NUM_NUMBERING]])


def unit_of_measure():
    """
    Generates global Units of Measure with descriptive, production-like explanations.
    """
    uom_descriptions = {
        "EA": "Individual unit of measure representing a single item.",
        "HR": "Unit of time representing one hour of labor or service.",
        "DAY": "Unit of time representing one full working day.",
        "BOX": "Packaging unit representing items packed in a box.",
        "KG": "Metric unit of weight representing one kilogram.",
        "LB": "Imperial unit of weight representing one pound.",
        "LTR": "Metric unit of volume representing one liter.",
        "MTR": "Metric unit of length representing one meter."
    }

    return pd.DataFrame([{
        "UOM": uom,
        "Description": uom_descriptions.get(uom, uom),
        "Active": True
    } for uom in list(uom_descriptions.keys())[:NUM_UOM]])


def contacts(customers_df, vendors_df):
    """
    Generate Contacts linked to Customers and Vendors with unique, realistic emails.
    - Emails are unique
    - Each Customer: 1–3 contacts
    - Each Vendor: 1 contact
    """
    contact_list = []
    email_set = set()  # Ensure uniqueness
    contact_id = 1
    domains = [
    # Global public providers
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "live.com",

    # Business focused providers
    "zoho.com",
    "protonmail.com",
    "fastmail.com",
    "icloud.com",

    # Corporate style domains
    "company.com",
    "enterprise.com",
    "businesscorp.com",
    "globaltech.com",
    "solutions.com",
    "servicesgroup.com",

    # Regional enterprise domains
    "ukbusiness.co.uk",
    "euoperations.eu",
    "apacgroup.com",
    "meconsulting.ae",
    "indiacorp.in",

    # Industry style domains
    "logisticsco.com",
    "manufacturinggroup.com",
    "retailsolutions.com",
    "financialservices.com"
]

    def make_unique_email(name):
        # Generate email using first.last pattern
        parts = name.split()
        if len(parts) < 2:
            first = parts[0]
            last = "user"
        else:
            first, last = parts[0], parts[-1]
        base_email = f"{first.lower()}.{last.lower()}"
        domain = random.choice(domains)
        email = f"{base_email}@{domain}"
        suffix = 2
        while email in email_set:
            email = f"{base_email}{suffix}@{domain}"
            suffix += 1
        email_set.add(email)
        return email

    # ------------------ Customer Contacts ------------------
    for _, cust in customers_df.iterrows():
        num_contacts = random.randint(1, 3)  # 1-3 contacts per customer
        for _ in range(num_contacts):
            name = fake.name()
            email = make_unique_email(name)
            contact_list.append({
                "ContactID": f"CT{contact_id:06}",
                "ContactName": name,
                "Email": email,
                "CustomerCD": cust["CustomerCD"],
                "VendorCD": None,
                "Active": True
            })
            contact_id += 1

    # ------------------ Vendor Contacts ------------------
    for _, vend in vendors_df.iterrows():
        name = fake.name()
        email = make_unique_email(name)
        contact_list.append({
            "ContactID": f"CT{contact_id:06}",
            "ContactName": name,
            "Email": email,
            "CustomerCD": None,
            "VendorCD": vend["VendorCD"],
            "Active": True
        })
        contact_id += 1

    return pd.DataFrame(contact_list)
#--------------------------------------------------------------------------

def main():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ------------------- MASTER DATA -------------------
    # Currency → Ledger → Branch → Warehouse
    cur  = deduplicate(currency(), ["CurrencyID"], "Currency")
    led  = deduplicate(ledger(cur), ["LedgerCD"], "Ledger")
    br   = deduplicate(branches(led), ["BranchCD"], "Branch")
    wh   = deduplicate(warehouse(br), ["WarehouseCD"], "Warehouse")
    # Accounts & SubAccounts → ItemClass → UnitOfMeasure → StockItem & NonStockItem
    acc  = deduplicate(accounts(), ["AccountCD"], "Account")
    sub  = deduplicate(subaccounts(), ["SubAccountCD"], "SubAccount")

    trm  = deduplicate(terms(), ["TermsID"], "Terms") 

    ic   = deduplicate(itemclass(acc, sub), ["ItemClassCD"], "ItemClass")
    uom  = deduplicate(unit_of_measure(), ["UOM"], "UnitOfMeasure")
    stk  = deduplicate(stockitem(ic, uom), ["InventoryCD"], "StockItem")
    nstk = deduplicate(nonstock_items(ic, uom), ["InventoryCD"], "NonStockItem")

    # Customer-related (FIXED ARGUMENTS)
    cc   = deduplicate(customer_class(acc, sub, trm), ["CustomerClassID"], "CustomerClass")
    cust = deduplicate(customers(cc, trm), ["CustomerCD"], "Customer")
    vend = deduplicate(vendors(trm), ["VendorCD"], "Vendor")

    cont = deduplicate(contacts(cust, vend), ["ContactID"], "Contact")
  # Employees and Projects
    emp  = deduplicate(employees(br), ["EmployeeCD"], "Employee")
    proj = deduplicate(projects(cust, br), ["ProjectCD"], "Project")

    num  = deduplicate(numbering(), ["NumberingID"], "Numbering")

    # ------------------- TRANSACTIONS -------------------
    # Sales Orders, Shipments, Invoices
    # 1️⃣ Generate Sales Orders
    # ---------------- SALES ORDERS ----------------
    so_header, so_line = sales_orders(cust, br, stk)

    so_header = deduplicate(so_header, ["OrderNbr"], "SalesOrderHeader")
    so_line = deduplicate(so_line, ["OrderNbr", "LineNbr"], "SalesOrderLine")

    # ---------------- SHIPMENTS ----------------
    shp_header, shp_line = shipments(so_header, so_line)

    shp_header = deduplicate(shp_header, ["ShipmentNbr"], "ShipmentHeader")
    shp_line = deduplicate(shp_line, ["ShipmentNbr", "LineNbr"], "ShipmentLine")

    inv  = deduplicate(sales_invoices(cust, br), ["InvoiceNbr"], "SalesInvoice")
    # Purchase Orders, Receipts, Bills

    po = deduplicate(
        purchase_orders(vend, br),
        ["OrderNbr"],
        "PurchaseOrder"
    )

    pol = deduplicate(
        purchase_order_lines(po, stk, nstk, wh, proj),
        ["OrderNbr", "LineNbr"],
        "PurchaseOrderLine"
    )

    pr = deduplicate(
        purchase_receipts(po, wh),
        ["ReceiptNbr"],
        "PurchaseReceipt"
    )

    bill = deduplicate(
        bills(vend, br, pr,po),
        ["RefNbr"],
        "Bill"
    )

    # General Ledger Transactions
    gl = journal_transactions(led, br, acc, sub, cur)

    check = gl.groupby("BatchNbr")[["DebitAmt", "CreditAmt"]].sum()
    unbalanced = check[check["DebitAmt"] != check["CreditAmt"]]
    assert unbalanced.empty, f"Unbalanced GL batches detected:\n{unbalanced.head()}"
    assert (check["DebitAmt"] == check["CreditAmt"]).all()



    # ------------------- EXPORT -------------------

    datasets = {
        "Currency.csv": cur,
        "Ledger.csv": led,
        "Branch.csv": br,
        "Warehouse.csv": wh,
        "Account.csv": acc,
        "SubAccount.csv": sub,
        "ItemClass.csv": ic,
        "StockItem.csv": stk,
        "NonStockItem.csv": nstk,
        "CustomerClass.csv": cc,
        "Customer.csv": cust,
        "Vendor.csv": vend,
        "Employee.csv": emp,
        "Project.csv": proj,
        "SalesOrderHeader.csv": so_header,
        "SalesOrderLine.csv": so_line,
        "ShipmentHeader.csv": shp_header,
        "ShipmentLine.csv": shp_line,
        "SalesInvoice.csv": inv,
        "PurchaseOrder.csv": pol,
        "PurchaseReceipt.csv": pr,
        "Bill.csv": bill,
        "JournalTransaction.csv": gl,
        "Terms.csv": trm,
        "Numbering.csv": num,
        "UnitOfMeasure.csv": uom,
        "Contact.csv": cont
    }
# Export all datasets to CSV
    for name, df in datasets.items():
        df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)
        print(f"✔ {name}: {len(df)} rows")

    print("\n✅ All Acumatica CSV files generated successfully with duplicates removed")

if __name__ == "__main__":
    main()
