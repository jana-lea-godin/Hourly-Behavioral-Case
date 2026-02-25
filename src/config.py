# src/config.py
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    root: Path
    data: Path
    results: Path
    reports: Path

    @property
    def results_tables(self) -> Path:
        return self.results / "tables"

    @property
    def results_alerts(self) -> Path:
        return self.results / "alerts"


def default_paths() -> Paths:
    root = Path(__file__).resolve().parents[1]
    return Paths(
        root=root,
        data=root / "data",
        results=root / "results",
        reports=root / "reports",
    )