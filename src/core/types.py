from dataclasses import dataclass
from enum import Enum


class SanitizationState(str, Enum):
    ACCEPT = "ACCEPT"
    REPAIR = "REPAIR"
    QUARANTINE = "QUARANTINE"


class DataTrustState(str, Enum):
    TRUSTED = "TRUSTED"
    DEGRADED = "DEGRADED"
    UNTRUSTED = "UNTRUSTED"


class HypothesisState(str, Enum):
    VALID = "VALID"
    WEAKENING = "WEAKENING"
    INVALID = "INVALID"


class DecisionState(str, Enum):
    ALLOWED = "ALLOWED"
    RESTRICTED = "RESTRICTED"
    HALTED = "HALTED"


@dataclass
class EngineState:
    sanitization: SanitizationState = SanitizationState.QUARANTINE
    data_trust: DataTrustState = DataTrustState.DEGRADED
    hypothesis: HypothesisState = HypothesisState.WEAKENING
    decision: DecisionState = DecisionState.RESTRICTED
