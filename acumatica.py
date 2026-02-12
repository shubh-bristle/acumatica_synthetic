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
NUM_BRANCHES = 100    #MasterData Increased quantity
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

def weighted_currency_choice(currency_df):
    """
    Returns a currency ID with higher probability
    for preferred Acumatica currencies.
    """
    weights = [
        5 if row["CurrencyID"] in PREFERRED_CURRENCIES else 1
        for _, row in currency_df.iterrows()
    ]

    return random.choices(
        currency_df["CurrencyID"].tolist(),
        weights=weights,
        k=1
    )[0]


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
                weighted_currency_choice(currency_df)
                if balance_type in ["Actual", "Budget"]
                else None
            ),
            "Active": True
        })

    return pd.DataFrame(ledgers)


def branch(ledger_df):
    """
    Generates 100 realistic Acumatica branches
    and assigns them ONLY to Actual ledgers.
    """

    # Only Actual ledgers can be assigned to branches
    actual_ledgers = ledger_df[
        ledger_df["BalanceType"] == "Actual"
    ]["LedgerCD"].tolist()

    # Base branch templates
    base_branches = [
        ("HQ", "Headquarters"),
        ("US_EAST", "US East Regional Office"),
        ("US_WEST", "US West Regional Office"),
        ("EMEA", "Europe Middle East Africa"),
        ("APAC", "Asia Pacific Regional Office"),
        ("LATAM", "Latin America Regional Office"),
        ("MFG", "Manufacturing Plant"),
        ("DC", "Distribution Center"),
        ("STORE", "Retail Store"),
        ("SERVICES", "Professional Services Division"),
        ("RND", "Research and Development"),
        ("SHARED", "Shared Services")
    ]

    branches = []
    branch_counter = 1

    # Generate branches until we reach 100
    while len(branches) < 100:
        for code, desc in base_branches:
            if len(branches) >= 100:
                break

            branch_cd = f"{code}{branch_counter:02}"
            description = f"{desc} {branch_counter:02}"

            branches.append({
                "BranchCD": branch_cd,
                "Description": description,
                "LedgerCD": random.choice(actual_ledgers),
                "Active": True
            })

            branch_counter += 1

    return pd.DataFrame(branches)

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

def sales_orders(customers_df, branch_df):
    global SALES_ORDER_LINES

    sales_orders_list = []
    sales_order_lines = []
    ORDER_TYPES = ["SO", "CM", "IN"]
    used_order_nbrs = set()

    STATUSES = ["Open", "Completed", "Cancelled", "Backordered"]
    SHIP_VIAS = ["DHL", "FedEx", "UPS", "BlueDart"]

    def generate_order_nbr(branch_cd, order_date):
        while True:
            suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            year = order_date.year
            order_nbr = f"SO-{branch_cd}-{year}-{suffix}"
            if order_nbr not in used_order_nbrs:
                used_order_nbrs.add(order_nbr)
                return order_nbr

    for _ in range(NUM_SALES_ORDERS):
        customer_cd = random.choice(customers_df["CustomerCD"])
        branch_cd = random.choice(branch_df["BranchCD"])
        order_date = fake.date_between(start_date="-24M", end_date="today")

        order_nbr = generate_order_nbr(branch_cd, order_date)

        

        # ----- HEADER -----
        sales_orders_list.append({
            "OrderNbr": order_nbr,
            "OrderType": random.choices(
                ORDER_TYPES, weights=[70, 15, 15]
            )[0],
            "CustomerCD": customer_cd,
            "OrderDate": order_date,
            "BranchCD": branch_cd,
            "Status": random.choices(
                STATUSES, weights=[50, 35, 10, 5]
            )[0],
            "ShipVia": random.choice(SHIP_VIAS),
            "RequestedDate": order_date + timedelta(days=random.randint(2, 14)),
            "ProjectID": None   # safe default, can be populated later
        })

        # ----- LINES (HIDDEN FROM CALLER) -----
        line_count = random.randint(1, 5)
        for line_nbr in range(1, line_count + 1):
            sales_order_lines.append({
                "OrderNbr": order_nbr,
                "LineNbr": line_nbr,
                "InventoryID": f"ITEM-{random.randint(1000,9999)}",
                "OrderQty": random.randint(1, 20),
                "UnitPrice": round(random.uniform(10, 500), 2)
            })

    SALES_ORDER_LINES = pd.DataFrame(sales_order_lines)

    return pd.DataFrame(sales_orders_list)


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

    def extract_branch(order_nbr):
        # Example: SO-IN-BLR-2025-BKWK → IN-BLR
        parts = order_nbr.split("-")
        return "-".join(parts[1:3])

    for _, so in sales_orders_df.iterrows():
        ship_date = fake.date_between(so["OrderDate"], "today")
        branch_cd = extract_branch(so["OrderNbr"])

        # Filter warehouses belonging to same branch
        eligible_wh = warehouse_df[
            warehouse_df["WarehouseCD"].str.contains(branch_cd)
        ]

        # Fallback only if no warehouse exists for that branch
        if eligible_wh.empty:
            eligible_wh = warehouse_df

        rows.append({
            "ShipmentNbr": gen_ship(ship_date),
            "OrderNbr": so["OrderNbr"],
            "WarehouseCD": random.choice(eligible_wh["WarehouseCD"].tolist()),
            "ShipmentDate": ship_date
        })

    return pd.DataFrame(rows)


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

    def generate_ref_nbr(branch_cd, bill_date):
        while True:
            suf = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            nbr = f"BILL-{branch_cd}-{bill_date.year}-{suf}"
            if nbr not in used_ref_nbrs:
                used_ref_nbrs.add(nbr)
                return nbr

    for _ in range(NUM_BILLS):

        vendor = vendors_df.sample(1).iloc[0]

        link_to_receipt = not receipts_df.empty and random.random() < 0.65

        if link_to_receipt:
            receipt = receipts_df.sample(1).iloc[0]
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


def journal_transactions(
    ledger_df, branch_df, account_df, subaccount_df, currency_df
):
    """
    Generates realistic Journal Transactions linked to ledgers,
    branches, accounts, and subaccounts.
    """

    journal_entries = []

    actual_ledgers = ledger_df[
        ledger_df["BalanceType"] == "Actual"
    ]["LedgerCD"].tolist()

    for i in range(NUM_JOURNAL_TRANSACTIONS):
        ledger_cd = random.choice(actual_ledgers)
        branch_cd = random.choice(branch_df["BranchCD"])
        account_cd = random.choice(account_df["AccountCD"])
        subaccount_cd = random.choice(subaccount_df["SubAccountCD"])

        currency_id = weighted_currency_choice(currency_df)

        debit = round(random.uniform(100, 5000), 2)
        credit = debit

        journal_entries.append({
            "BatchNbr": f"JB{i+1:06}",
            "LedgerCD": ledger_cd,
            "BranchCD": branch_cd,
            "AccountCD": account_cd,
            "SubAccountCD": subaccount_cd,
            "CurrencyID": currency_id,
            "DebitAmt": debit,
            "CreditAmt": credit,
            "TransactionDate": fake.date_between(start_date="-12M", end_date="today"),
            "Active": True
        })

    return pd.DataFrame(journal_entries)

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
    br   = deduplicate(branch(led), ["BranchCD"], "Branch")
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
    so   = deduplicate(sales_orders(cust, br), ["OrderNbr"], "SalesOrder")
    shp  = deduplicate(shipments(so, wh), ["ShipmentNbr"], "Shipment")
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
    gl   = deduplicate(
    journal_transactions(led, br, acc, sub, cur),
    ["BatchNbr", "AccountCD", "SubAccountCD", "TransactionDate"],
    "JournalTransaction"
)

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
        "SalesOrder.csv": so,
        "Shipment.csv": shp,
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
