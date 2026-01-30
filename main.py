import os

# ================= CONFIG =================
from config.settings import OUTPUT_DIR

# ================= UTILS =================
from utils.deduplication import deduplicate

# ================= MASTER DATA =================
from master_data.currency import currency
from master_data.ledger import ledger
from master_data.branch import branch
from master_data.warehouse import warehouse

from master_data.accounts import accounts
from master_data.subaccounts import subaccounts
from master_data.itemclass import itemclass
from master_data.unit_of_measure import unit_of_measure
from master_data.stock_items import stockitem
from master_data.nonstock_items import nonstock_items

from master_data.terms import terms
from master_data.customer_class import customer_class
from master_data.customers import customers
from master_data.vendors import vendors
from master_data.contacts import contacts

from master_data.employees import employees
from master_data.projects import projects
from master_data.numbering import numbering

# ================= TRANSACTIONS =================
from transactions.sales_orders import sales_orders
from transactions.shipments import shipments
from transactions.sales_invoices import sales_invoices

from transactions.purchase_orders import purchase_orders
from transactions.purchase_receipts import purchase_receipts
from transactions.bills import bills

from transactions.journal_transactions import journal_transactions

# ================= EXPORT =================
from export.csv_exporter import export_all


def main():
    # -------------------------------------------------
    # Create output directory
    # -------------------------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # -------------------------------------------------
    # MASTER DATA GENERATION (ORDER MATTERS)
    # -------------------------------------------------

    # Currency → Ledger → Branch → Warehouse
    cur = deduplicate(currency(), ["CurrencyID"], "Currency")
    led = deduplicate(ledger(cur), ["LedgerCD"], "Ledger")
    br = deduplicate(branch(led), ["BranchCD"], "Branch")
    wh = deduplicate(warehouse(br), ["WarehouseCD"], "Warehouse")

    # Accounts & SubAccounts
    acc = deduplicate(accounts(), ["AccountCD"], "Account")
    sub = deduplicate(subaccounts(), ["SubAccountCD"], "SubAccount")

    # Terms
    trm = deduplicate(terms(), ["TermsID"], "Terms")

    # Inventory Structure
    ic = deduplicate(itemclass(acc, sub), ["ItemClassCD"], "ItemClass")
    uom = deduplicate(unit_of_measure(), ["UOM"], "UnitOfMeasure")
    stk = deduplicate(stockitem(ic, uom), ["InventoryCD"], "StockItem")
    nstk = deduplicate(nonstock_items(ic, uom), ["InventoryCD"], "NonStockItem")

    # Customers & Vendors
    cc = deduplicate(
        customer_class(acc, sub, trm),
        ["CustomerClassID"],
        "CustomerClass"
    )

    cust = deduplicate(customers(cc, trm), ["CustomerCD"], "Customer")
    vend = deduplicate(vendors(trm), ["VendorCD"], "Vendor")

    cont = deduplicate(contacts(cust, vend), ["ContactID"], "Contact")

    # Employees & Projects
    emp = deduplicate(employees(br), ["EmployeeCD"], "Employee")
    proj = deduplicate(projects(cust, br), ["ProjectCD"], "Project")

    # Numbering Sequences
    num = deduplicate(numbering(), ["NumberingID"], "Numbering")

    # -------------------------------------------------
    # TRANSACTIONS
    # -------------------------------------------------

    # Sales
    so = deduplicate(sales_orders(cust, br), ["OrderNbr"], "SalesOrder")
    shp = deduplicate(shipments(so, wh), ["ShipmentNbr"], "Shipment")
    inv = deduplicate(sales_invoices(cust, br), ["InvoiceNbr"], "SalesInvoice")

    # Purchasing
    po = deduplicate(purchase_orders(vend, br), ["OrderNbr"], "PurchaseOrder")
    pr = deduplicate(purchase_receipts(po, wh), ["ReceiptNbr"], "PurchaseReceipt")
    bill_df = deduplicate(bills(vend, br), ["RefNbr"], "Bill")

    # General Ledger
    gl = deduplicate(
        journal_transactions(led, br, acc, sub, cur),
        ["BatchNbr", "AccountCD", "SubAccountCD", "TransactionDate"],
        "JournalTransaction"
    )

    # -------------------------------------------------
    # EXPORT ALL DATASETS
    # -------------------------------------------------

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
        "Contact.csv": cont,

        "Employee.csv": emp,
        "Project.csv": proj,

        "SalesOrder.csv": so,
        "Shipment.csv": shp,
        "SalesInvoice.csv": inv,

        "PurchaseOrder.csv": po,
        "PurchaseReceipt.csv": pr,
        "Bill.csv": bill_df,

        "JournalTransaction.csv": gl,

        "Terms.csv": trm,
        "Numbering.csv": num,
        "UnitOfMeasure.csv": uom
    }

    export_all(datasets, OUTPUT_DIR)

    print("\n✅ All Acumatica CSV files generated successfully")


if __name__ == "__main__":
    main()
