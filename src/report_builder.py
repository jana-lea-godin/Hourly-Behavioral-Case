# src/report_builder.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


@dataclass
class BudgetConfig:
    # hard limit how many "do this today" items we show
    max_focus_items: int = 3
    # maximum watchlist rows in table
    max_watchlist_rows: int = 12


class ReportBuilder:
    def __init__(
        self,
        action_queue_path: Path,
        output_path: Path,
        budget: BudgetConfig = BudgetConfig(),
    ) -> None:
        self.action_queue_path = action_queue_path
        self.output_path = output_path
        self.budget = budget

    def load_items(self) -> List[Dict[str, Any]]:
        if not self.action_queue_path.exists():
            return []
        return json.loads(self.action_queue_path.read_text(encoding="utf-8"))

    @staticmethod
    def _section(title: str) -> str:
        return f"\n\n## {title}\n\n"

    @staticmethod
    def _as_float(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except Exception:
            return default

    @staticmethod
    def _as_int(x: Any, default: int = 0) -> int:
        try:
            return int(x)
        except Exception:
            return default

    def _priority_key(self, item: Dict[str, Any]) -> Tuple[int, float]:
        """
        Higher is better.
        Use severity first, then confidence.
        """
        return (self._as_int(item.get("severity", 0)), self._as_float(item.get("confidence", 0.0)))

    def _bucket(self, items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        b: Dict[str, List[Dict[str, Any]]] = {
            "Risks": [],
            "Opportunities": [],
            "Watchlist": [],
            "Other": [],
        }

        for x in items:
            t = x.get("issue_type", "Other")
            if t in ("EarlyWarning", "DataQuality", "Drift", "Instability"):
                b["Risks"].append(x)
            elif t == "Opportunity":
                b["Opportunities"].append(x)
            elif t == "Watchlist":
                b["Watchlist"].append(x)
            else:
                b["Other"].append(x)

        # Sort each bucket by our priority key
        for k in b:
            b[k] = sorted(b[k], key=self._priority_key, reverse=True)

        return b

    def _render_focus_list(self, items: List[Dict[str, Any]]) -> str:
        """
        Renders a compact "today" list, obeying alert budget.
        """
        if not items:
            return ""

        top = items[: self.budget.max_focus_items]
        lines = ["**What to do next (today):**"]
        for x in top:
            metric = x.get("metric", "")
            action = x.get("recommended_action", "")
            lines.append(f"- {metric}: **{action}**")
        if len(items) > len(top):
            lines.append(
                f"\n_Alert budget: showing top {len(top)} items to prevent noise. "
                f"{len(items) - len(top)} additional items are listed below._"
            )
        return "\n".join(lines) + "\n"

    def _render_opportunities(self, items: List[Dict[str, Any]]) -> str:
        if not items:
            return ""
        out: List[str] = [self._section("🟢 Opportunities")]

        for x in items[: self.budget.max_focus_items]:
            out.append(f"### {x.get('metric','')}")
            out.append(f"- **Signal:** {x.get('signal','')}")
            out.append(f"- **Impact:** {x.get('expected_impact','')}")
            out.append(f"- **Why now:** {x.get('why_now','')}")
            extra = x.get("extra", {}) or {}
            if "breach_examples" in extra and extra["breach_examples"]:
                be = ", ".join(map(str, extra["breach_examples"][:6]))
                out.append(f"- **Breach examples:** {be}")
            out.append(f"- **Action:** {x.get('recommended_action','')}")
            out.append(f"- **Owner:** {x.get('owner_hint','')}")
            out.append(f"- **Confidence:** {x.get('confidence','')}")
            out.append("")  # blank line

        if len(items) > self.budget.max_focus_items:
            out.append(
                f"_Showing top {self.budget.max_focus_items} opportunities. "
                f"Remaining {len(items)-self.budget.max_focus_items} are deprioritized by alert budget._\n"
            )
        return "\n".join(out)

    def _render_risks(self, items: List[Dict[str, Any]]) -> str:
        if not items:
            return ""
        out: List[str] = [self._section("🔴 Risks")]

        for x in items[: self.budget.max_focus_items]:
            out.append(f"### {x.get('metric','')}")
            out.append(f"- **Signal:** {x.get('signal','')}")
            out.append(f"- **Impact:** {x.get('expected_impact','')}")
            out.append(f"- **Why now:** {x.get('why_now','')}")
            out.append(f"- **Action:** {x.get('recommended_action','')}")
            out.append(f"- **Owner:** {x.get('owner_hint','')}")
            out.append(f"- **Confidence:** {x.get('confidence','')}")
            extra = x.get("extra", {}) or {}
            rc = extra.get("root_cause") if isinstance(extra, dict) else None
            if isinstance(rc, dict) and rc.get("reason"):
                out.append(f"- **Root-cause hint:** `{rc.get('reason')}`")
            out.append("")  # blank line

        if len(items) > self.budget.max_focus_items:
            out.append(
                f"_Showing top {self.budget.max_focus_items} risks. "
                f"Remaining {len(items)-self.budget.max_focus_items} are deprioritized by alert budget._\n"
            )
        return "\n".join(out)

    def _render_watchlist_table(self, items: List[Dict[str, Any]]) -> str:
        if not items:
            return ""

        rows = items[: self.budget.max_watchlist_rows]
        out: List[str] = [self._section("🟡 Watchlist")]

        out.append("| Metric | Signal | Impact | Owner | Confidence |")
        out.append("|---|---|---:|---|---:|")
        for x in rows:
            out.append(
                f"| {x.get('metric','')} | {x.get('signal','')} | {x.get('expected_impact','')} | {x.get('owner_hint','')} | {x.get('confidence','')} |"
            )

        if len(items) > len(rows):
            out.append(
                f"\n_Only showing top {len(rows)} watchlist rows. "
                f"{len(items)-len(rows)} more exist in action_queue.csv._\n"
            )
        return "\n".join(out)

    def build(self) -> None:
        items = self.load_items()
        b = self._bucket(items)

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        risks = b["Risks"]
        opps = b["Opportunities"]
        watch = b["Watchlist"]

        content: List[str] = []
        content.append("# Weekly Behavioral Monitoring Brief")
        content.append(f"_Generated: {now}_\n")

        # topline
        content.append(
            f"**Topline:** 🔴 {len(risks)} risk(s) / 🟢 {len(opps)} opportunity(s) / 🟡 {len(watch)} watchlist item(s)\n"
        )

        # "today" list obeying budget: prioritize risks then opps
        focus_pool = sorted((risks + opps), key=self._priority_key, reverse=True)
        if focus_pool:
            content.append(self._render_focus_list(focus_pool))
        else:
            content.append("**What to do next (today):**\n- No critical items. Review watchlist and keep monitoring.\n")

        # sections
        r = self._render_risks(risks)
        if r:
            content.append(r)

        o = self._render_opportunities(opps)
        if o:
            content.append(o)

        w = self._render_watchlist_table(watch)
        if w:
            content.append(w)

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text("\n".join(content).strip() + "\n", encoding="utf-8")
        