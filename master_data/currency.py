import random
import pandas as pd
import pycountry
from config.settings import NUM_CURRENCIES
from utils.currency_utils import PREFERRED_CURRENCIES

def currency():
    currencies = []
    seen = set()
    all_currencies = list(pycountry.currencies)
    random.shuffle(all_currencies)

    for curr in all_currencies:
        if curr.alpha_3 in PREFERRED_CURRENCIES and curr.alpha_3 not in seen:
            currencies.append({
                "CurrencyID": curr.alpha_3,
                "Description": curr.name,
                "DecimalPlaces": 0 if curr.alpha_3 in {"JPY", "KRW"} else 2,
                "Active": True
            })
            seen.add(curr.alpha_3)

    for curr in all_currencies:
        if len(currencies) >= NUM_CURRENCIES:
            break
        if not hasattr(curr, "alpha_3"):
            continue
        if curr.alpha_3 in seen:
            continue

        currencies.append({
            "CurrencyID": curr.alpha_3,
            "Description": curr.name,
            "DecimalPlaces": 0 if curr.alpha_3 in {"JPY", "KRW"} else 2,
            "Active": True
        })
        seen.add(curr.alpha_3)

    return pd.DataFrame(currencies)
