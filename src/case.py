# src/case.py
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.action_queue import ActionQueueBuilder
from src.config import default_paths
from src.early_warning import EarlyWarningSystem
from src.feature_store import FeatureStore


def run() -> None:
    # Paths
    paths = default_paths()

    # 1) Load
    df = pd.read_csv(paths.data / "raw" / "dataset_ecommerce_hourly.csv")

    # 2) Build features (clean + datetime + z-scores)
    fs = (
        FeatureStore(df)
        .clean_percent_columns()
        .prepare_datetime()
        .rolling_baseline("Buyers_Orders_Created")   # Outcome
        .rolling_baseline("CR_Orders_Created")       # Quality measure
    )
    df_features = fs.get_df()

    # Optional: quick debug print (can remove later)
    print(
        df_features[
            [
                "Buyers_Orders_Created",
                "Buyers_Orders_Created_zscore",
                "CR_Orders_Created",
                "CR_Orders_Created_zscore",
            ]
        ].tail(10)
    )

    # 3) Detect early warnings
    ews = EarlyWarningSystem(df_features)
    alerts_orders: List[Dict[str, Any]] = ews.detect_zscore_alerts("Buyers_Orders_Created")
    alerts_cr: List[Dict[str, Any]] = ews.detect_zscore_alerts("CR_Orders_Created")
    all_alerts = alerts_orders + alerts_cr

    # 4) Build action queue (customer-ready output)
    builder = ActionQueueBuilder()
    builder.extend(all_alerts)
    items = builder.build()

    # 5) Save outputs
    json_path = paths.results_alerts / "action_queue.json"
    csv_path = paths.results_tables / "action_queue.csv"

    builder.to_json(items, json_path)
    builder.to_csv(items, csv_path)

    print(f"✅ Wrote {len(items)} action items")
    print(f"- JSON: {json_path}")
    print(f"- CSV : {csv_path}")


if __name__ == "__main__":
    run()