import pandas as pd
from pathlib import Path


DATA_PATH = Path("data/raw/dataset_ecommerce_hourly.csv")


def run_audit():
    print("Loading data...")
    df = pd.read_csv(DATA_PATH)

    print("\n--- BASIC INFO ---")
    print(df.info())

    print("\n--- HEAD ---")
    print(df.head())

    print("\n--- DATE RANGE ---")
    df["Datetime"] = pd.to_datetime(df["Datetime"])
    print("Min:", df["Datetime"].min())
    print("Max:", df["Datetime"].max())
    print("Total hours:", len(df))

    print("\n--- NULL CHECK ---")
    print(df.isna().mean().sort_values(ascending=False))

    print("\n--- UNIQUE VALUE CHECK (low cardinality columns) ---")
    for col in df.columns:
        unique_vals = df[col].nunique()
        if unique_vals < 10:
            print(col, "->", unique_vals, "unique values")


if __name__ == "__main__":
    run_audit()