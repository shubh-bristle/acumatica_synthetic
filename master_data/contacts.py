import random
import pandas as pd
from utils.faker_setup import fake

def contacts(customers_df, vendors_df):
    rows = []
    email_set = set()
    contact_id = 1

    domains = [
        "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
        "company.com", "enterprise.com", "businesscorp.com",
        "globaltech.com", "solutions.com", "servicesgroup.com",
        "ukbusiness.co.uk", "euoperations.eu", "apacgroup.com",
        "meconsulting.ae", "indiacorp.in"
    ]

    def unique_email(name):
        parts = name.split()
        first = parts[0].lower()
        last = parts[-1].lower() if len(parts) > 1 else "user"
        base = f"{first}.{last}"
        domain = random.choice(domains)
        email = f"{base}@{domain}"
        i = 2
        while email in email_set:
            email = f"{base}{i}@{domain}"
            i += 1
        email_set.add(email)
        return email

    # Customer contacts (1–3 per customer)
    for _, cust in customers_df.iterrows():
        for _ in range(random.randint(1, 3)):
            name = fake.name()
            rows.append({
                "ContactID": f"CT{contact_id:06}",
                "ContactName": name,
                "Email": unique_email(name),
                "CustomerCD": cust["CustomerCD"],
                "VendorCD": None,
                "Active": True
            })
            contact_id += 1

    # Vendor contacts (1 per vendor)
    for _, vend in vendors_df.iterrows():
        name = fake.name()
        rows.append({
            "ContactID": f"CT{contact_id:06}",
            "ContactName": name,
            "Email": unique_email(name),
            "CustomerCD": None,
            "VendorCD": vend["VendorCD"],
            "Active": True
        })
        contact_id += 1

    return pd.DataFrame(rows)
