import logging
from typing import Any, Dict, Optional

from src.adapters.base import Event, Stream
from src.core.types import (
    EngineState,
    SanitizationState,
    DataTrustState,
    HypothesisState,
    DecisionState,
)
from src.core.stats import EngineStats
from src.core.sanitization import Sanitizer
from src.core.time_alignment import TimeAligner
from src.core.data_trust import DataTrustPolicy
from src.core.hypothesis import HypothesisPolicy
from src.core.decision import DecisionMachine
from src.orderbook.replayer import OrderBookReplayer
from src.utils.time import now_us

logger = logging.getLogger(__name__)


class SingleDecisionEngine:
    def __init__(self, cfg: Dict[str, Any], writer):
        self.writer = writer

        self.state = EngineState()

        self.last_ingest_ts_by_stream: Dict[Stream, Optional[int]] = {s: None for s in Stream}

        engine_cfg = cfg["engine"]
        self.trades_stall_threshold_us = engine_cfg["trades_stall_threshold_ms"] * 1000
        self.orderbook_stall_threshold_us = engine_cfg["orderbook_stall_threshold_ms"] * 1000
        self.liquidations_stall_threshold_us = engine_cfg["liquidations_stall_threshold_ms"] * 1000
        self.ticker_stall_threshold_us = engine_cfg["ticker_stall_threshold_ms"] * 1000

        depth_limit = cfg["adapters"]["ws"]["depth_snapshot_limit"]
        self.lob_replayer = OrderBookReplayer(depth_limit)

        aligner_cfg = cfg["time_alignment"]
        self.aligner = TimeAligner(aligner_cfg)

        sanitizer_cfg = cfg["sanitization"]
        exchange = cfg["exchange"]
        symbol = cfg["symbol"]
        self.sanitizer = Sanitizer(sanitizer_cfg, exchange, symbol)

        data_trust_cfg = cfg["data_trust"]
        self.data_trust = DataTrustPolicy(data_trust_cfg, self.lob_replayer)

        hypothesis_cfg = cfg["hypothesis"]
        self.hypothesis = HypothesisPolicy(hypothesis_cfg, self.lob_replayer)

        self.decision = DecisionMachine()

        self.stats = EngineStats()
        self.stats.init_dwell(now_us(), self.state.sanitization, self.state.data_trust, self.state.hypothesis, self.state.decision)

        self._emit_state_transition(now_us(), "engine_init")

        logger.info("SingleDecisionEngine initialized")

    def ingest(self, ev: Event) -> None:
        now_ts = now_us()
        self.last_ingest_ts_by_stream[ev.stream] = now_ts

        aligned_evs, align_stats = self.aligner.align(ev)
        self.data_trust.on_batch(ev.stream, align_stats)

        for aligned_ev in aligned_evs:
            logger.info(aligned_ev)

            sanitization, fixed_ev, sanitization_trigger = self.sanitizer.sanitize(aligned_ev)
            self._set_sanitization(sanitization, now_ts)

            if aligned_ev.stream == Stream.ORDERBOOK and sanitization != SanitizationState.QUARANTINE:
                self.lob_replayer.on_event(aligned_ev, now_ts)

            data_trust, data_trust_trigger = self.data_trust.on_event(fixed_ev.stream, sanitization, aligned_ev)
            self._set_data_trust(data_trust, now_ts)

            hypothesis, hypothesis_trigger = self.hypothesis.verify(fixed_ev, now_ts)
            self._set_hypothesis(hypothesis, now_ts)

            trigger = " | ".join(
                p for p in [
                    f"hypothesis:{hypothesis_trigger}" if hypothesis_trigger else "",
                    f"data_trust:{data_trust_trigger}" if data_trust_trigger else "",
                    f"sanitization:{sanitization_trigger}" if sanitization_trigger else "",
                ]
                if p
            )
            decision = self._set_decision(now_ts, trigger)
            self.stats.on_event(sanitization, data_trust, hypothesis, decision)
            
            logger.info(f"Decision(decision={decision.value} hypothesis={hypothesis.value} data_trust={data_trust.value} sanitization={sanitization.value} | trigger={ {trigger} })")

    def tick(self, now_ts: int) -> None:
        stalled_streams = []
        for stream, ingest_ts in self.last_ingest_ts_by_stream.items():
            if ingest_ts is None:
                continue

            match stream:
                case Stream.TRADES:
                    if now_ts - ingest_ts > self.trades_stall_threshold_us:
                        stalled_streams.append(stream)
                case Stream.ORDERBOOK:
                    if now_ts - ingest_ts > self.orderbook_stall_threshold_us:
                        stalled_streams.append(stream)
                case Stream.LIQUIDATIONS:
                    if now_ts - ingest_ts > self.liquidations_stall_threshold_us:
                        stalled_streams.append(stream)
                case Stream.TICKER:
                    if now_ts - ingest_ts > self.ticker_stall_threshold_us:
                        stalled_streams.append(stream)

        if stalled_streams:
            sanitization = self.state.sanitization

            data_trust = DataTrustState.DEGRADED
            self._set_data_trust(data_trust, now_ts)

            hypothesis = HypothesisState.WEAKENING
            self._set_hypothesis(hypothesis, now_ts)

            trigger = f"stall:{','.join(s.value for s in stalled_streams)}"
            decision = self._set_decision(now_ts, trigger)
            self.stats.on_event(sanitization, data_trust, hypothesis, decision)

            logger.info(f"Decision(decision={decision} hypothesis={hypothesis.value} data_trust={data_trust.value} sanitization={sanitization.value} | trigger={ {trigger} })")

    def shutdown(self) -> None:
        now_ts = now_us()
        self._emit_decision(now_ts, self.decision.last_ts, self.state.decision, self.decision.last_reason)

        summary = self.stats.finalize(now_ts)
        self.writer.write_summary(summary)

        logger.info(f"SingleDecisionEngine closed")

    def _set_sanitization(self, state: SanitizationState, now_ts: int) -> None:
        if state == self.state.sanitization:
            return
        
        self.state.sanitization = state
        self.stats.switch_san(now_ts, state)

    def _set_data_trust(self, state: DataTrustState, now_ts: int) -> None:
        if state == self.state.data_trust:
            return
        
        self.state.data_trust = state
        self.stats.switch_trust(now_ts, state)

    def _set_hypothesis(self, state: HypothesisState, now_ts: int) -> None:
        if state == self.state.hypothesis:
            return

        self.state.hypothesis = state
        self.stats.switch_hypo(now_ts, state)

    def _set_decision(self, now_ts: int, trigger: str) -> DecisionState:
        prev_decision = self.state.decision
        prev_decision_ts = self.decision.last_ts
        prev_reason = self.decision.last_reason

        decision = self.decision.compute(
            data_trust=self.state.data_trust,
            hypothesis=self.state.hypothesis,
        )
        if decision != prev_decision or trigger != prev_reason:
            self.state.decision = decision
            self.decision.last_ts = now_ts
            self.decision.last_reason = trigger

            self.stats.switch_decision(now_ts, decision)
            self._emit_decision(now_ts, prev_decision_ts, prev_decision, prev_reason)
        
        self._emit_state_transition(now_ts, trigger)
        return decision

    def _emit_state_transition(self, now_ts: int, trigger: str) -> None:
        rec = {
            "ts": now_ts,
            "data_trust": self.state.data_trust.value,
            "hypothesis": self.state.hypothesis.value,
            "decision": self.state.decision.value,
            "trigger": trigger,
        }
        self.writer.write_state_transition(rec)

    def _emit_decision(self, now_ts: int, prev_decision_ts: int, prev_decision: DecisionState, prev_reason: str) -> None:
        duration_ms = (now_ts - prev_decision_ts) // 1000 if prev_decision_ts is not None else 0
        rec = {
            "ts": now_ts,
            "action": prev_decision.value,
            "reason": prev_reason,
            "duration_ms": duration_ms,
        }
        self.writer.write_decision(rec)
