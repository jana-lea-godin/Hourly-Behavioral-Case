# src/case.py
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.action_queue import ActionQueueBuilder
from src.config import default_paths
from src.early_warning import EarlyWarningSystem
from src.feature_store import FeatureStore
from src.root_cause import RootCauseAnalyzer


def run() -> None:
    
    paths = default_paths()

    # 1) Load
    df = pd.read_csv(paths.data / "raw" / "dataset_ecommerce_hourly.csv")

    # 2) Build features (clean + datetime + z-scores for funnel + outcome/quality)
    fs = (
        FeatureStore(df)
        .clean_percent_columns()
        .prepare_datetime()
        .rolling_baseline("Visitors")
        .rolling_baseline("Products_Viewed")
        .rolling_baseline("Add_to_Cart_Visitors")
        .rolling_baseline("CR_Products_Added_to_Cart")
        .rolling_baseline("Buyers_Orders_Created")  # Outcome
        .rolling_baseline("CR_Orders_Created")      # Quality measure
    )
    df_features = fs.get_df()

    # Optional debug print (remove later)
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

    # Root-cause analyzer (uses df_features + z-scores of funnel metrics)
    rca = RootCauseAnalyzer(df_features)

    # 3) Detect warnings/opportunities (hard + ewma + frequency)
    ews = EarlyWarningSystem(df_features)

    # Hard rule (strict blocks)
    hard_orders: List[Dict[str, Any]] = ews.detect_zscore_alerts(
        "Buyers_Orders_Created",
        threshold=2.0,
        min_duration=3,
        direction="down",
    )
    hard_cr: List[Dict[str, Any]] = ews.detect_zscore_alerts(
        "CR_Orders_Created",
        threshold=2.0,
        min_duration=3,
        direction="both",
    )

    # EWMA rule (trend-sensitive)
    ewma_orders: List[Dict[str, Any]] = ews.detect_ewma_alerts(
        "Buyers_Orders_Created",
        alpha=0.2,
        threshold=1.5,
        min_duration=6,
        direction="down",
    )
    ewma_cr: List[Dict[str, Any]] = ews.detect_ewma_alerts(
        "CR_Orders_Created",
        alpha=0.2,
        threshold=1.5,
        min_duration=6,
        direction="both",
    )

    # Frequency rule (intermittent issues, not necessarily consecutive)
    freq_orders: List[Dict[str, Any]] = ews.detect_frequency_alerts(
        "Buyers_Orders_Created",
        window_hours=24,
        threshold=2.0,
        min_hits=4,
        direction="down",
    )

    # CR frequency: split into down (risk) and up (opportunity)
    freq_cr_down: List[Dict[str, Any]] = ews.detect_frequency_alerts(
        "CR_Orders_Created",
        window_hours=24,
        threshold=2.0,
        min_hits=4,
        direction="down",
    )

    freq_cr_up: List[Dict[str, Any]] = ews.detect_frequency_alerts(
        "CR_Orders_Created",
        window_hours=24,
        threshold=2.0,
        min_hits=4,
        direction="up",
    )

    # Tag positive CR spikes as Opportunity instead of EarlyWarning
    for e in freq_cr_up:
        e["issue_type"] = "Opportunity"
        e["expected_impact"] = "High"
        e["recommended_action"] = (
            "Conversion spike detected – analyze campaign, traffic mix, and replicate drivers"
        )
        e["owner_hint"] = "Growth/Marketing"
        e["why_now"] = "Unusually strong conversion efficiency detected (potential upside)"

    # Combine all events
    all_events: List[Dict[str, Any]] = (
        hard_orders
        + hard_cr
        + ewma_orders
        + ewma_cr
        + freq_orders
        + freq_cr_down
        + freq_cr_up
    )

    # If no alerts, still provide an actionable watchlist
    if len(all_events) == 0:
        print("ℹ️ No sustained alerts found — generating watchlist items instead.")
        all_events += ews.build_watchlist("Buyers_Orders_Created", top_k=5)
        all_events += ews.build_watchlist("CR_Orders_Created", top_k=5)
    elif len(all_events) < 5:
        print("ℹ️ Few alerts found — adding watchlist context items.")
        all_events += ews.build_watchlist("Buyers_Orders_Created", top_k=3)
        all_events += ews.build_watchlist("CR_Orders_Created", top_k=3)

    # Optional debug summaries
    print("Summary:", ews.summary("Buyers_Orders_Created"))
    print("Summary:", ews.summary("CR_Orders_Created"))

    # 4) Enrich EarlyWarning events with root-cause hints + sharper action text
    for e in all_events:
        extra = e.get("extra", {}) or {}
        if e.get("issue_type") == "EarlyWarning" and "start" in extra and "end" in extra:
            diag = rca.diagnose_orders_drop(extra["start"], extra["end"])
            reason = diag.get("reason", "unknown")

            # Default enrichment
            e["extra"]["root_cause"] = diag

            # Policy: refine actions based on reason
            if reason == "traffic_drop":
                e["owner_hint"] = "Marketing/Growth"
                e["recommended_action"] = (
                    "Investigate traffic drop: check campaigns, channel delivery, SEO/paid outages, and geo/device mix"
                )
                e["verification_step"] = (
                    "Compare Visitors + Products_Viewed vs 7d baseline; inspect channel mix changes around breach hours"
                )
            elif reason == "checkout_friction":
                e["owner_hint"] = "Engineering + UX"
                e["recommended_action"] = (
                    "Investigate checkout friction: validate payment provider, error logs, and recent deploys"
                )
                e["verification_step"] = (
                    "Check CR_Orders_Created and error rates; correlate breach hours with deployments/incidents"
                )
            elif reason == "product_discovery_or_pdp_friction":
                e["owner_hint"] = "UX/Product"
                e["recommended_action"] = (
                    "Investigate PDP/search friction: check page speed, search clicks, product views, and add-to-cart behavior"
                )
                e["verification_step"] = (
                    "Compare Products_Viewed, Add_to_Cart_Visitors, CR_Products_Added_to_Cart vs baseline around breach hours"
                )
            elif reason == "no_clear_funnel_driver":
                e["owner_hint"] = "Analytics + Tracking + Engineering"
                e["recommended_action"] = (
                    "No single funnel step stands out. Run triage: "
                    "(1) check tracking/definitions, (2) check traffic mix shift, (3) check intermittent checkout errors"
                )
                e["verification_step"] = (
                    "Triage checklist: "
                    "A) validate Orders_Created logging + tag firing, "
                    "B) compare Visitors vs Orders ratio by hour-of-day, "
                    "C) scan for incidents/deploys during breach_examples hours"
                )
            else:
                # keep suggested owner/next_check from diag
                e["owner_hint"] = diag.get("owner_suggested", e.get("owner_hint", "Analytics"))
                e["verification_step"] = diag.get(
                    "next_check", e.get("verification_step", "Inspect funnel metrics manually")
                )

    # 5) Build action queue
    builder = ActionQueueBuilder()
    builder.extend(all_events)
    items = builder.build()

    # 6) Save outputs
    json_path = paths.results_alerts / "action_queue.json"
    csv_path = paths.results_tables / "action_queue.csv"

    builder.to_json(items, json_path)
    builder.to_csv(items, csv_path)

    print(f"✅ Wrote {len(items)} action items")
    print(f"- JSON: {json_path}")
    print(f"- CSV : {csv_path}")


if __name__ == "__main__":
    run()