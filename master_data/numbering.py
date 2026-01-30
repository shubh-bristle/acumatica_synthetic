import pandas as pd
from config.settings import NUM_NUMBERING

def numbering():
    numbering_metadata = {
        "SO": "Automatically generates unique identifiers for Sales Orders.",
        "PO": "Automatically generates unique identifiers for Purchase Orders.",
        "INV": "Automatically generates unique identifiers for AR Invoices.",
        "BILL": "Automatically generates unique identifiers for AP Bills.",
        "GL": "Automatically generates unique batch numbers for GL journals.",
        "CUST": "Automatically generates unique IDs for Customer records."
    }

    rows = []

    for prefix in list(numbering_metadata.keys())[:NUM_NUMBERING]:
        rows.append({
            "NumberingID": f"{prefix}_SEQ",
            "Description": numbering_metadata[prefix],
            "NewSymbol": prefix,
            "StartNbr": "000001",
            "EndNbr": "999999",
            "LastNbr": "000000",
            "Active": True
        })

    return pd.DataFrame(rows)
