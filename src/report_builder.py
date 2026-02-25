# src/report_builder.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class Buckets:
    risks: List[Dict[str, Any]]
    opportunities: List[Dict[str, Any]]
    watchlist: List[Dict[str, Any]]


class ReportBuilder:
    def __init__(self, action_queue_path: Path, output_path: Path):
        self.action_queue_path = action_queue_path
        self.output_path = output_path

    def load_items(self) -> List[Dict[str, Any]]:
        if not self.action_queue_path.exists():
            return []
        return json.loads(self.action_queue_path.read_text(encoding="utf-8"))

    @staticmethod
    def _bucket(items: List[Dict[str, Any]]) -> Buckets:
        risks = [x for x in items if x.get("issue_type") in ("EarlyWarning", "DataQuality")]
        opportunities = [x for x in items if x.get("issue_type") == "Opportunity"]
        watchlist = [x for x in items if x.get("issue_type") == "Watchlist"]
        return Buckets(risks=risks, opportunities=opportunities, watchlist=watchlist)

    @staticmethod
    def _h2(title: str) -> str:
        return f"\n## {title}\n"

    @staticmethod
    def _fmt_conf(x: Dict[str, Any]) -> str:
        try:
            return f"{float(x.get('confidence', 0.0)):.2f}"
        except Exception:
            return str(x.get("confidence", ""))

    @staticmethod
    def _breach_examples(item: Dict[str, Any], max_n: int = 6) -> Optional[str]:
        extra = item.get("extra") or {}
        examples = extra.get("breach_examples")
        if not examples:
            return None
        return ", ".join(examples[:max_n])

    @staticmethod
    def _topline(b: Buckets) -> str:
        parts = []
        if b.risks:
            parts.append(f"🔴 {len(b.risks)} risk(s)")
        if b.opportunities:
            parts.append(f"🟢 {len(b.opportunities)} opportunity(s)")
        if b.watchlist:
            parts.append(f"🟡 {len(b.watchlist)} watchlist item(s)")
        return " / ".join(parts) if parts else "No signals detected."

    @staticmethod
    def _next_actions(b: Buckets) -> List[str]:
        actions: List[str] = []
        if b.risks:
            r = b.risks[0]
            actions.append(f"Address risk: **{r['metric']}** → {r['recommended_action']}")
        if b.opportunities:
            o = b.opportunities[0]
            actions.append(f"Exploit opportunity: **{o['metric']}** → {o['recommended_action']}")
        if not actions and b.watchlist:
            w = b.watchlist[0]
            actions.append(f"Monitor: **{w['metric']}** → {w['signal']}")
        return actions

    @staticmethod
    def _format_opportunity(item: Dict[str, Any]) -> str:
        lines = [
            f"### {item['metric']}",
            f"- **Signal:** {item['signal']}",
            f"- **Impact:** {item['expected_impact']}",
            f"- **Why now:** {item.get('why_now','')}",
        ]
        ex = ReportBuilder._breach_examples(item)
        if ex:
            lines.append(f"- **Breach examples:** {ex}")
        lines += [
            f"- **Action:** {item['recommended_action']}",
            f"- **Owner:** {item['owner_hint']}",
            f"- **Confidence:** {ReportBuilder._fmt_conf(item)}",
        ]
        return "  \n".join(lines) + "  \n"

    @staticmethod
    def _format_risk(item: Dict[str, Any]) -> str:
        lines = [
            f"### {item['metric']}",
            f"- **Signal:** {item['signal']}",
            f"- **Severity:** {item['severity']} / 5",
            f"- **Impact:** {item['expected_impact']}",
            f"- **Why now:** {item.get('why_now','')}",
        ]
        ex = ReportBuilder._breach_examples(item)
        if ex:
            lines.append(f"- **Breach examples:** {ex}")
        lines += [
            f"- **Action:** {item['recommended_action']}",
            f"- **Owner:** {item['owner_hint']}",
            f"- **Verify:** {item.get('verification_step','')}",
            f"- **Confidence:** {ReportBuilder._fmt_conf(item)}",
        ]
        return "  \n".join(lines) + "  \n"

    @staticmethod
    def _watchlist_table(items: List[Dict[str, Any]], max_rows: int = 12) -> str:
        rows = items[:max_rows]
        if not rows:
            return "_No watchlist items._\n"

        out = [
            "| Metric | Signal | Impact | Owner | Confidence |",
            "|---|---|---:|---|---:|",
        ]
        for x in rows:
            out.append(
                f"| {x.get('metric','')} | {x.get('signal','')} | {x.get('expected_impact','')} | "
                f"{x.get('owner_hint','')} | {ReportBuilder._fmt_conf(x)} |"
            )
        return "\n".join(out) + "\n"

    def build(self) -> None:
        items = self.load_items()
        b = self._bucket(items)

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        content: List[str] = []
        content.append("# Weekly Behavioral Monitoring Brief")
        content.append(f"_Generated: {now}_")
        content.append("")
        content.append(f"**Topline:** {self._topline(b)}")
        content.append("")

        next_actions = self._next_actions(b)
        if next_actions:
            content.append("**What to do next (today):**")
            for a in next_actions:
                content.append(f"- {a}")
            content.append("")

        if b.risks:
            content.append(self._h2("🔴 Risks"))
            for r in b.risks:
                content.append(self._format_risk(r))

        if b.opportunities:
            content.append(self._h2("🟢 Opportunities"))
            for o in b.opportunities:
                content.append(self._format_opportunity(o))

        content.append(self._h2("🟡 Watchlist"))
        content.append(self._watchlist_table(b.watchlist, max_rows=12))

        if not items:
            content.append("\n_No alerts or signals detected._\n")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text("\n".join(content).strip() + "\n", encoding="utf-8")
