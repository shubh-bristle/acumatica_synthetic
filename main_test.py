from testingfeatures.sync_data import sync_with_existing
from master_data.currency import currency
from master_data.ledger import ledger

def main():
    # Define your file paths
    CURRENCY_FILE = r"C:\Users\shubham.sanwariya\python learn\acumatica_1M_Data\Currency.csv"
    LEDGER_FILE = r"C:\Users\shubham.sanwariya\python learn\acumatica_1M_Data\Ledger.csv"

    # --- PROCESS CURRENCIES ---
    # 1. Generate fresh data
    fresh_currencies = currency() 
    
    # 2. Sync with what's already on the hard drive
    final_currencies = sync_with_existing(fresh_currencies, CURRENCY_FILE, ["CurrencyID"], "Currency")
    
    # 3. Save the updated master list back to the CSV
    final_currencies.to_csv(CURRENCY_FILE, index=False)

    # --- PROCESS LEDGERS ---
    # 4. Generate fresh ledgers (using the synced currencies)
    fresh_ledgers = ledger(final_currencies)
    
    # 5. Sync with existing ledgers
    final_ledgers = sync_with_existing(fresh_ledgers, LEDGER_FILE, ["LedgerCD"], "Ledger")
    
    # 6. Save back to CSV
    final_ledgers.to_csv(LEDGER_FILE, index=False)

    print("✅ Sync complete!")

main()