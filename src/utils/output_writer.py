import json
import threading
from pathlib import Path
from typing import Any, Dict


class OutputWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self.state_file = open(self.output_dir / "state_transitions.jsonl", "w", encoding="utf-8")
        self.decision_file = open(self.output_dir / "decisions.jsonl", "w", encoding="utf-8")

        self.summary: Dict[str, Any] = {}

    def write_state_transition(self, record: Dict[str, Any]) -> None:
        with self._lock:
            self.state_file.write(json.dumps(record) + "\n")
            self.state_file.flush()

    def write_decision(self, record: dict) -> None:
        with self._lock:
            self.decision_file.write(json.dumps(record) + "\n")
            self.decision_file.flush()

    def write_summary(self, record: Dict[str, Any]) -> None:
        with self._lock:
            self.summary.update(record)

    def finalize(self) -> None:
        with self._lock:
            self.state_file.flush()
            self.state_file.close()
            self.decision_file.flush()
            self.decision_file.close()

            with open(self.output_dir / "summary.json", "w", encoding="utf-8") as f:
                json.dump(self.summary, f, ensure_ascii=False, indent=2)
