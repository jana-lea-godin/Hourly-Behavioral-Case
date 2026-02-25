# src/action_queue.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import csv
import json


@dataclass(frozen=True)
class ActionItem:
    timestamp_generated: str
    scope: str                    # global | segment | device | channel ...
    segment_id: Optional[str]      # None for global
    issue_type: str                # EarlyWarning | Opportunity | Drift | Instability | DataQuality | Watchlist
    severity: int                  # 1-5
    confidence: float              # 0-1
    metric: str
    signal: str
    expected_impact: str           # High | Med | Low
    why_now: str
    recommended_action: str
    owner_hint: str
    verification_step: str
    stop_rule: str
    extra: Dict[str, Any]


class ActionQueueBuilder:
    """
    Takes alerts/drift/instability events and turns them into an ordered list of ActionItems.
    Start simple: accept already-normalized "events" dicts.
    Later: plug in EarlyWarningSystem + DriftDetector outputs.
    """

    def __init__(self) -> None:
        self._items: List[ActionItem] = []

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def add_event(self, event: Dict[str, Any]) -> None:
        """
        Expected minimal event schema (you can extend anytime):
        {
          "scope": "global" | "segment" | ...,
          "segment_id": "HighIntent" | None,
          "issue_type": "EarlyWarning" | "Opportunity" | "Watchlist" | ...,
          "severity": 1..5,
          "confidence": 0..1,
          "metric": "...",
          "signal": "...",
          "expected_impact": "High" | "Med" | "Low",
          "why_now": "...",
          "recommended_action": "...",
          "owner_hint": "...",
          "verification_step": "...",
          "stop_rule": "...",
          "extra": {...}
        }
        """
        item = ActionItem(
            timestamp_generated=event.get("timestamp_generated") or self.now_iso(),
            scope=event["scope"],
            segment_id=event.get("segment_id"),
            issue_type=event["issue_type"],
            severity=int(event.get("severity", 1)),
            confidence=float(event.get("confidence", 0.5)),
            metric=event.get("metric", ""),
            signal=event.get("signal", ""),
            expected_impact=event.get("expected_impact", "Med"),
            why_now=event.get("why_now", ""),
            recommended_action=event.get("recommended_action", ""),
            owner_hint=event.get("owner_hint", "Unknown"),
            verification_step=event.get("verification_step", ""),
            stop_rule=event.get("stop_rule", ""),
            extra=event.get("extra", {}) or {},
        )
        self._items.append(item)

    def extend(self, events: Iterable[Dict[str, Any]]) -> None:
        for e in events:
            self.add_event(e)

    @staticmethod
    def _type_priority(issue_type: str) -> int:
        """
        Customer-friendly priority:
        - Risk / urgent issues first
        - Opportunities next
        - Monitoring/watchlist last
        """
        priority_map = {
            "DataQuality": 4,
            "EarlyWarning": 4,   # risk
            "Drift": 3,
            "Instability": 3,
            "Opportunity": 2,    # upside
            "Watchlist": 1,      # monitoring
        }
        return priority_map.get(issue_type, 0)

    def build(self) -> List[ActionItem]:
        """
        Sort order:
        1) issue_type priority (risk > opportunity > watchlist)
        2) severity (desc)
        3) confidence (desc)
        4) newest first (desc) as tie-breaker
        """

        def sort_key(x: ActionItem) -> Tuple[int, int, float, str]:
            return (
                self._type_priority(x.issue_type),
                x.severity,
                x.confidence,
                x.timestamp_generated,
            )

        return sorted(self._items, key=sort_key, reverse=True)

    @staticmethod
    def to_json(items: List[ActionItem], out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = [asdict(i) for i in items]
        out_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @staticmethod
    def to_csv(items: List[ActionItem], out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [asdict(i) for i in items]

        # flatten "extra" as JSON string for CSV
        for r in rows:
            r["extra"] = json.dumps(r.get("extra", {}), ensure_ascii=False)

        fieldnames = list(rows[0].keys()) if rows else [
            "timestamp_generated", "scope", "segment_id", "issue_type", "severity", "confidence",
            "metric", "signal", "expected_impact", "why_now", "recommended_action", "owner_hint",
            "verification_step", "stop_rule", "extra",
        ]

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)