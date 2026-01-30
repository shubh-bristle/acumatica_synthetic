import os

def export_all(datasets, output_dir):
    for name, df in datasets.items():
        df.to_csv(os.path.join(output_dir, name), index=False)
        print(f"✔ {name}: {len(df)} rows")
