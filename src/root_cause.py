# src/root_cause.py
from __future__ import annotations

from typing import Dict, Any, Optional
import pandas as pd


class RootCauseAnalyzer:
    """
    Root-cause hinting based on average z-scores in an alert window.
    Not a causal proof — but a practical, explainable triage tool.
    """

    def __init__(self, df_features: pd.DataFrame):
        self.df = df_features

    def diagnose_orders_drop(self, start: str, end: str) -> Dict[str, Any]:
        window = self.df.loc[start:end].copy()

        def mean_z(metric: str) -> Optional[float]:
            zc = f"{metric}_zscore"
            if zc not in window.columns:
                return None
            s = window[zc].dropna()
            return None if s.empty else float(s.mean())

        # Funnel & outcome metrics we can attribute to
        metrics = [
            "Visitors",
            "Products_Viewed",
            "Add_to_Cart_Visitors",
            "CR_Products_Added_to_Cart",
            "CR_Orders_Created",
            "Buyers_Orders_Created",
        ]

        avg = {m: mean_z(m) for m in metrics}

        # Choose the strongest negative deviation (excluding outcome itself for attribution)
        candidates = {k: v for k, v in avg.items() if v is not None and k != "Buyers_Orders_Created"}
        strongest_metric = None
        strongest_value = None
        if candidates:
            strongest_metric, strongest_value = min(candidates.items(), key=lambda kv: kv[1])

        # Interpret strongest metric into a reason/owner
        reason = "unknown"
        owner = "Analytics"
        next_check = "Inspect funnel metrics manually"

        # threshold for "meaningful" average deviation in the window (not too strict)
        meaningful = -0.75

        if strongest_metric is None or strongest_value is None:
            reason = "insufficient_data"
            owner = "Analytics"
            next_check = "Check baseline availability (first 7 days have NaNs)"
        elif strongest_value < meaningful:
            if strongest_metric == "Visitors":
                reason = "traffic_drop"
                owner = "Marketing/Growth"
                next_check = "Check campaigns, channel mix, ad delivery, SEO outages"
            elif strongest_metric in {"Products_Viewed", "Add_to_Cart_Visitors", "CR_Products_Added_to_Cart"}:
                reason = "product_discovery_or_pdp_friction"
                owner = "UX/Product"
                next_check = "Check PDP/search performance, pricing/shipping clarity, mobile UX, page speed"
            elif strongest_metric == "CR_Orders_Created":
                reason = "checkout_friction"
                owner = "Engineering + UX"
                next_check = "Check checkout errors, payment provider status, recent deploys, speed metrics"
            else:
                reason = "funnel_shift"
                owner = "UX/Product"
                next_check = "Inspect funnel step changes and anomalies"
        else:
            # Nothing stands out strongly → suggest data/definition sanity check
            reason = "no_clear_funnel_driver"
            owner = "Tracking/Engineering"
            next_check = "Validate event definitions, tagging, and whether order creation logging changed"

        return {
            "avg_z": avg,
            "strongest_negative_driver": {
                "metric": strongest_metric,
                "avg_z": strongest_value,
                "threshold_meaningful": meaningful,
            },
            "reason": reason,
            "owner_suggested": owner,
            "next_check": next_check,
        }