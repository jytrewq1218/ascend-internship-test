from typing import Any, Dict, Optional

from src.core.types import DataTrustState, HypothesisState, DecisionState


class DecisionMachine:
    def __init__(self):
        self.last_ts: Optional[int] = None
        self.last_reason: Optional[str] = None

    def compute(
        self,
        data_trust: DataTrustState,
        hypothesis: HypothesisState,
    ) -> DecisionState:
        if data_trust == DataTrustState.DEGRADED or hypothesis == HypothesisState.WEAKENING:
            return DecisionState.RESTRICTED

        if data_trust == DataTrustState.UNTRUSTED or hypothesis == HypothesisState.INVALID:
            return DecisionState.HALTED

        return DecisionState.ALLOWED
