import random

PREFERRED_CURRENCIES = {
    "USD", "GBP", "INR", "AED", "CAD",
    "SAR", "AUD", "PKR", "BRL", "EGP"
}

def weighted_currency_choice(currency_df):
    weights = [
        5 if row["CurrencyID"] in PREFERRED_CURRENCIES else 1
        for _, row in currency_df.iterrows()
    ]
    return random.choices(
        currency_df["CurrencyID"].tolist(),
        weights=weights,
        k=1
    )[0]
