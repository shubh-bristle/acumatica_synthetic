def deduplicate(df, keys, entity_name):
    before = len(df)
    df = df.drop_duplicates(subset=keys, keep="first")
    removed = before - len(df)
    if removed > 0:
        print(f"⚠ Removed {removed} duplicate rows from {entity_name}")
    return df
