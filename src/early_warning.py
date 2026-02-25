# src/early_warning.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal
import pandas as pd

Direction = Literal["both", "down", "up"]


class EarlyWarningSystem:
    def __init__(self, df: pd.DataFrame):
        # expects Datetime index + *_zscore columns present
        self.df = df.copy()

    def detect_zscore_alerts(
        self,
        column: str,
        threshold: float = 2.0,
        min_duration: int = 3,
        direction: Direction = "both",
        scope: str = "global",
        segment_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Finds sustained deviations in z-score.
        direction:
          - both: abs(z) > threshold
          - down: z < -threshold
          - up:   z > threshold
        """
        z_col = f"{column}_zscore"
        if z_col not in self.df.columns:
            raise ValueError(f"Missing z-score column: {z_col}")

        df = self.df.copy()

        if direction == "both":
            df["alert_flag"] = df[z_col].abs() > threshold
            df["peak_strength"] = df[z_col].abs()
            dir_label = "±"
        elif direction == "down":
            df["alert_flag"] = df[z_col] < -threshold
            df["peak_strength"] = (-df[z_col]).clip(lower=0)
            dir_label = "-"
        else:  # up
            df["alert_flag"] = df[z_col] > threshold
            df["peak_strength"] = (df[z_col]).clip(lower=0)
            dir_label = "+"

        # group consecutive True blocks
        df["group"] = (df["alert_flag"] != df["alert_flag"].shift()).cumsum()

        alerts: List[Dict[str, Any]] = []
        for _, block in df[df["alert_flag"]].groupby("group"):
            if len(block) < min_duration:
                continue

            peak = float(block["peak_strength"].max())
            severity = self._map_severity(peak)
            confidence = self._confidence(peak, len(block))

            alerts.append(
                {
                    "scope": scope,
                    "segment_id": segment_id,
                    "issue_type": "EarlyWarning",
                    "severity": severity,
                    "confidence": round(confidence, 2),
                    "metric": column,
                    "signal": f"{dir_label} z peaked at {peak:.2f} over {len(block)}h (thr={threshold})",
                    "expected_impact": "High" if column == "Buyers_Orders_Created" else "Med",
                    "why_now": f"Deviation sustained for {len(block)} consecutive hours",
                    "recommended_action": self._action_template(column),
                    "owner_hint": self._owner_template(column),
                    "verification_step": self._verification_template(column),
                    "stop_rule": self._stop_rule_template(column),
                    "extra": {
                        "start": str(block.index.min()),
                        "end": str(block.index.max()),
                        "threshold": threshold,
                        "min_duration": min_duration,
                        "direction": direction,
                        "method": "zscore_block",
                    },
                }
            )

        return alerts

    def detect_ewma_alerts(
        self,
        column: str,
        alpha: float = 0.2,
        threshold: float = 1.5,
        min_duration: int = 6,
        direction: Direction = "both",
        scope: str = "global",
        segment_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        EWMA on z-scores to detect sustained trend shifts with less noise.
        Uses the existing {column}_zscore series.
        """
        z_col = f"{column}_zscore"
        if z_col not in self.df.columns:
            raise ValueError(f"Missing z-score column: {z_col}")

        s = self.df[z_col].dropna()
        if s.empty:
            return []

        ewma = s.ewm(alpha=alpha, adjust=False).mean()

        if direction == "both":
            flag = ewma.abs() > threshold
            strength = ewma.abs()
            dir_label = "±"
        elif direction == "down":
            flag = ewma < -threshold
            strength = (-ewma).clip(lower=0)
            dir_label = "-"
        else:
            flag = ewma > threshold
            strength = (ewma).clip(lower=0)
            dir_label = "+"

        tmp = pd.DataFrame({"ewma": ewma, "flag": flag, "strength": strength})
        tmp["group"] = (tmp["flag"] != tmp["flag"].shift()).cumsum()

        alerts: List[Dict[str, Any]] = []
        for _, block in tmp[tmp["flag"]].groupby("group"):
            if len(block) < min_duration:
                continue

            peak = float(block["strength"].max())
            # Align severity with typical z-threshold scale
            severity = self._map_severity(max(peak, 2.0))
            confidence = min(1.0, (peak / 2.5) * (len(block) / 12.0))

            alerts.append(
                {
                    "scope": scope,
                    "segment_id": segment_id,
                    "issue_type": "EarlyWarning",
                    "severity": severity,
                    "confidence": round(confidence, 2),
                    "metric": column,
                    "signal": f"EWMA(alpha={alpha}) {dir_label} peaked at {peak:.2f} over {len(block)}h (thr={threshold})",
                    "expected_impact": "High" if column == "Buyers_Orders_Created" else "Med",
                    "why_now": f"EWMA deviation sustained for {len(block)} hours",
                    "recommended_action": self._action_template(column),
                    "owner_hint": self._owner_template(column),
                    "verification_step": self._verification_template(column),
                    "stop_rule": self._stop_rule_template(column),
                    "extra": {
                        "start": str(block.index.min()),
                        "end": str(block.index.max()),
                        "alpha": alpha,
                        "threshold": threshold,
                        "min_duration": min_duration,
                        "direction": direction,
                        "method": "ewma",
                    },
                }
            )

        return alerts


    def detect_frequency_alerts(
        self,
        column: str,
        window_hours: int = 24,
        threshold: float = 2.0,
        min_hits: int = 4,
        direction: Direction = "both",
        scope: str = "global",
        segment_id: Optional[str] = None,
        max_examples: int = 8,
        max_alerts: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Frequency-based alerts: within a rolling window, count how many hours breach threshold.
        Catches intermittent issues (not necessarily consecutive).

        Improvements:
        - include example breach timestamps for faster investigation
        - cap number of alerts returned (top severity/confidence first)
        """
        z_col = f"{column}_zscore"
        if z_col not in self.df.columns:
            raise ValueError(f"Missing z-score column: {z_col}")

        s = self.df[z_col].dropna()
        if s.empty:
            return []

        if direction == "both":
            hit = s.abs() > threshold
            dir_label = "±"
            strength_series = s.abs()
        elif direction == "down":
            hit = s < -threshold
            dir_label = "-"
            strength_series = (-s).clip(lower=0)
        else:
            hit = s > threshold
            dir_label = "+"
            strength_series = (s).clip(lower=0)

        hits = hit.rolling(window=window_hours, min_periods=window_hours).sum()
        flag = hits >= min_hits

        tmp = pd.DataFrame({"hits": hits, "flag": flag})
        tmp["group"] = (tmp["flag"] != tmp["flag"].shift()).cumsum()

        alerts: List[Dict[str, Any]] = []

        for _, block in tmp[tmp["flag"]].groupby("group"):
            peak_hits = int(block["hits"].max())
            start = block.index.min()
            end = block.index.max()

            # collect breach timestamps within [start-window_hours+1, end]
            # (approximate investigation window)
            window_start = start - pd.Timedelta(hours=window_hours - 1)
            window_end = end
            hit_slice = hit.loc[(hit.index >= window_start) & (hit.index <= window_end)]
            breaches = hit_slice[hit_slice].index

            # take strongest breaches as examples
            if len(breaches) > 0:
                strengths = strength_series.loc[breaches].sort_values(ascending=False)
                breach_examples = [str(ts) for ts in strengths.head(max_examples).index]
                peak_strength = float(strengths.head(1).iloc[0])
            else:
                breach_examples = []
                peak_strength = float("nan")

            # severity by peak_hits
            if peak_hits >= min_hits + 3:
                severity = 5
            elif peak_hits >= min_hits + 1:
                severity = 4
            else:
                severity = 3

            confidence = min(1.0, peak_hits / (min_hits + 3))

            alerts.append(
                {
                    "scope": scope,
                    "segment_id": segment_id,
                    "issue_type": "EarlyWarning",
                    "severity": severity,
                    "confidence": round(confidence, 2),
                    "metric": column,
                    "signal": (
                        f"{dir_label} {peak_hits} breaches in {window_hours}h "
                        f"(thr={threshold}, min_hits={min_hits})"
                    ),
                    "expected_impact": "High" if column == "Buyers_Orders_Created" else "Med",
                    "why_now": f"Intermittent anomaly pattern detected over rolling {window_hours}h windows",
                    "recommended_action": self._action_template(column),
                    "owner_hint": self._owner_template(column),
                    "verification_step": self._verification_template(column),
                    "stop_rule": self._stop_rule_template(column),
                    "extra": {
                        "start": str(start),
                        "end": str(end),
                        "window_hours": window_hours,
                        "threshold": threshold,
                        "min_hits": min_hits,
                        "direction": direction,
                        "method": "frequency",
                        "peak_strength": peak_strength,
                        "breach_examples": breach_examples,
                    },
                }
            )

        # rank alerts and cap
        alerts_sorted = sorted(alerts, key=lambda a: (a["severity"], a["confidence"]), reverse=True)
        return alerts_sorted[:max_alerts]



    def build_watchlist(
        self,
        column: str,
        top_k: int = 5,
        scope: str = "global",
    ) -> List[Dict[str, Any]]:
        """
        If no sustained alerts, still output actionable monitoring items:
        pick top-k absolute z-scores (excluding NaNs).
        """
        z_col = f"{column}_zscore"
        s = self.df[z_col].dropna()
        if s.empty:
            return []

        top = s.abs().sort_values(ascending=False).head(top_k)
        items: List[Dict[str, Any]] = []
        for ts, absz in top.items():
            items.append(
                {
                    "scope": scope,
                    "segment_id": None,
                    "issue_type": "Watchlist",
                    "severity": 2 if absz >= 1.5 else 1,
                    "confidence": round(min(0.8, absz / 2.5), 2),
                    "metric": column,
                    "signal": f"|z|={absz:.2f} at {ts}",
                    "expected_impact": "Med" if column == "Buyers_Orders_Created" else "Low",
                    "why_now": "No sustained breach; monitoring top deviations",
                    "recommended_action": f"Monitor {column} around {ts} and check funnel steps if it repeats",
                    "owner_hint": self._owner_template(column),
                    "verification_step": self._verification_template(column),
                    "stop_rule": "If |z| > 2 for 3h → raise alert",
                    "extra": {"timestamp": str(ts), "abs_z": float(absz), "method": "watchlist"},
                }
            )
        return items

    def summary(self, column: str) -> Dict[str, Any]:
        z_col = f"{column}_zscore"
        s = self.df[z_col].dropna()
        if s.empty:
            return {"column": column, "count": 0}
        return {
            "column": column,
            "count": int(s.shape[0]),
            "max_abs_z": float(s.abs().max()),
            "pct_over_2": float((s.abs() > 2).mean()),
            "pct_over_1_5": float((s.abs() > 1.5).mean()),
        }

    @staticmethod
    def _map_severity(peak_abs_z: float) -> int:
        if peak_abs_z > 3:
            return 5
        if peak_abs_z > 2.5:
            return 4
        return 3

    @staticmethod
    def _confidence(peak_abs_z: float, duration_h: int) -> float:
        return min(1.0, (peak_abs_z / 3.0) * (duration_h / 6.0))

    @staticmethod
    def _owner_template(column: str) -> str:
        if column in {"Buyers_Orders_Created", "CR_Orders_Created"}:
            return "Growth/Marketing + UX"
        return "Analytics"

    @staticmethod
    def _action_template(column: str) -> str:
        if column == "Buyers_Orders_Created":
            return "Investigate conversion drop: check add-to-cart, checkout issues, traffic mix, and tracking health"
        if column == "CR_Orders_Created":
            return "Investigate funnel efficiency: check cart→order friction, pricing/shipping clarity, checkout errors"
        return f"Investigate deviation in {column}"

    @staticmethod
    def _verification_template(column: str) -> str:
        return "Validate volumes vs 7d baseline; check upstream funnel KPIs; rule out tracking anomalies"

    @staticmethod
    def _stop_rule_template(column: str) -> str:
        return "If sustained >12h or repeats within 48h → escalate and assign owner"