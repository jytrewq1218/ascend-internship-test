from dataclasses import dataclass, field
from typing import Dict, Optional

from src.core.types import SanitizationState, DataTrustState, HypothesisState, DecisionState


@dataclass
class DwellTracker:
    current: str
    enter_us: int

    total_us: Dict[str, int] = field(default_factory=dict)
    entries: Dict[str, int] = field(default_factory=dict)

    def switch(self, new: str, now_us: int) -> None:
        if new == self.current:
            return
        dt = max(0, now_us - self.enter_us)

        self.total_us[self.current] = self.total_us.get(self.current, 0) + dt
        self.entries[self.current] = self.entries.get(self.current, 0) + 1

        self.current = new
        self.enter_us = now_us

    def close(self, now_us: int) -> None:
        dt = max(0, now_us - self.enter_us)
        self.total_us[self.current] = self.total_us.get(self.current, 0) + dt
        self.entries[self.current] = self.entries.get(self.current, 0) + 1

    def snapshot(self) -> dict:
        avg_us = {
            state: (
                self.total_us[state] // max(1, self.entries.get(state, 0))
            )
            for state in self.total_us
        }

        return {
            "total_ts": dict(self.total_us),
            "avg_ts": avg_us,
        }


@dataclass
class EngineStats:
    total_events: int = 0
    quarantine_events: int = 0
    repair_events: int = 0

    trusted_events: int = 0
    degraded_events: int = 0
    untrusted_events: int = 0

    allowed_events: int = 0
    restricted_events: int = 0
    halted_events: int = 0

    valid_hypo_events: int = 0
    weakening_hypo_events: int = 0
    invalid_hypo_events: int = 0

    san_dwell: Optional[DwellTracker] = None
    trust_dwell: Optional[DwellTracker] = None
    hypo_dwell: Optional[DwellTracker] = None
    decision_dwell: Optional[DwellTracker] = None

    def on_event(self, san: SanitizationState, trust: DataTrustState, hypo: HypothesisState, decision: DecisionState) -> None:
        self.total_events += 1

        match san:
            case SanitizationState.ACCEPT:
                pass
            case SanitizationState.REPAIR:
                self.repair_events += 1
            case SanitizationState.QUARANTINE:
                self.quarantine_events += 1

        match trust:
            case DataTrustState.TRUSTED:
                self.trusted_events += 1
            case DataTrustState.DEGRADED:
                self.degraded_events += 1
            case DataTrustState.UNTRUSTED:
                self.untrusted_events += 1

        match decision:
            case DecisionState.ALLOWED:
                self.allowed_events += 1
            case DecisionState.RESTRICTED:
                self.restricted_events += 1
            case DecisionState.HALTED:
                self.halted_events += 1

        match hypo:
            case HypothesisState.VALID:
                self.valid_hypo_events += 1
            case HypothesisState.WEAKENING:
                self.weakening_hypo_events += 1
            case HypothesisState.INVALID:
                self.invalid_hypo_events += 1

    def init_dwell(self, now_us: int, san: SanitizationState, trust: DataTrustState, hypo: HypothesisState, decision: DecisionState) -> None:
        self.san_dwell = DwellTracker(san.value, now_us)
        self.trust_dwell = DwellTracker(trust.value, now_us)
        self.hypo_dwell = DwellTracker(hypo.value, now_us)
        self.decision_dwell = DwellTracker(decision.value, now_us)

    def switch_san(self, now_us: int, san: SanitizationState) -> None:
        if self.san_dwell:
            self.san_dwell.switch(san.value, now_us)

    def switch_trust(self, now_us: int, trust: DataTrustState) -> None:
        if self.trust_dwell:
            self.trust_dwell.switch(trust.value, now_us)

    def switch_hypo(self, now_us: int, hypo: HypothesisState) -> None:
        if self.hypo_dwell:
            self.hypo_dwell.switch(hypo.value, now_us)

    def switch_decision(self, now_us: int, decision: DecisionState) -> None:
        if self.decision_dwell:
            self.decision_dwell.switch(decision.value, now_us)

    def finalize(self, now_us: int) -> dict:
        if self.trust_dwell:
            self.trust_dwell.close(now_us)
        if self.hypo_dwell:
            self.hypo_dwell.close(now_us)
        if self.decision_dwell:
            self.decision_dwell.close(now_us)

        san_dwell_stats = self.san_dwell.snapshot() if self.san_dwell else {}
        trust_dwell_stats = self.trust_dwell.snapshot() if self.trust_dwell else {}
        hypo_dwell_stats = self.hypo_dwell.snapshot() if self.hypo_dwell else {}
        decision_dwell_stats = self.decision_dwell.snapshot() if self.decision_dwell else {}

        return {
            "total_events": self.total_events,
            "quarantine_events": self.quarantine_events,
            "repair_events": self.repair_events,

            "events_by_state": {
                "data_trust": {
                    "TRUSTED": self.trusted_events,
                    "DEGRADED": self.degraded_events,
                    "UNTRUSTED": self.untrusted_events,
                },
                "hypothesis": {
                    "VALID": self.valid_hypo_events,
                    "WEAKENING": self.weakening_hypo_events,
                    "INVALID": self.invalid_hypo_events,
                },
                "decision": {
                    "ALLOWED": self.allowed_events,
                    "RESTRICTED": self.restricted_events,
                    "HALTED": self.halted_events,
                },
            },

            "dwell": {
                "sanitization": san_dwell_stats,
                "data_trust": trust_dwell_stats,
                "hypothesis": hypo_dwell_stats,
                "decision": decision_dwell_stats,
            },
        }
