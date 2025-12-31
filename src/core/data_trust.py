from collections import deque
from typing import Any, Deque, Dict, Tuple, Optional

from src.adapters.base import Stream, Event
from src.core.types import DataTrustState, SanitizationState
from src.core.time_alignment import TimeAlignmentStats
from src.orderbook.replayer import OrderBookReplayer


class DataTrustPolicy:
    def __init__(self, cfg: Dict[str, Any], lob_replayer: OrderBookReplayer):
        self.window_events = cfg["window_events"]
        self.quarantine_untrusted_rate = cfg["quarantine_untrusted_rate"]
        self.late_degraded_rate = cfg["late_degraded_rate"]
        self.late_untrusted_rate = cfg["late_untrusted_rate"]
        self.forced_flush_degraded_rate = cfg["forced_flush_degraded_rate"]
        self.forced_flush_untrusted_rate = cfg["forced_flush_untrusted_rate"]
        self.buffer_len_degraded = cfg["buffer_len_degraded"]
        self.buffer_len_untrusted = cfg["buffer_len_untrusted"]
        self.spread_explode_bps = cfg["spread_explode_bps"]
        self.fat_finger_untrusted_bps = cfg["fat_finger_untrusted_bps"]
        self.fat_finger_degraded_bps = cfg["fat_finger_degraded_bps"]
        self.trade_jump_degraded_bps = cfg["trade_jump_degraded_bps"]

        self.lob_replayer = lob_replayer

        self._last_event_id: Dict[Stream, Any] = {}
        self._last_event_ts: Dict[Stream, Optional[int]] = {}

        self._align_window: Dict[Stream, Deque[Tuple[int, int, int, int]]] = {s: deque() for s in Stream}
        self._san_quarantine_window: Dict[Stream, Deque[int]] = {s: deque() for s in Stream}

        self._align_emitted_sum: Dict[Stream, int] = {s: 0 for s in Stream}
        self._align_late_sum: Dict[Stream, int] = {s: 0 for s in Stream}
        self._align_forced_sum: Dict[Stream, int] = {s: 0 for s in Stream}
        self._last_buffer_len: Dict[Stream, int] = {s: 0 for s in Stream}

        self._san_quarantine_sum: Dict[Stream, int] = {s: 0 for s in Stream}

        self._last_trade_price: Optional[float] = None

        self._state_by_stream: Dict[Stream, DataTrustState] = {s: DataTrustState.TRUSTED for s in Stream}
        self._reason_by_stream: Dict[Stream, str] = {s: "" for s in Stream}

    def on_batch(self, stream: Stream, stats: TimeAlignmentStats) -> None:
        emitted = stats.emitted
        late = stats.late
        forced = 1 if stats.forced_flush else 0
        buffer_len = stats.buffer_len

        self._align_window[stream].append((emitted, late, forced, buffer_len))
        self._align_emitted_sum[stream] += emitted
        self._align_late_sum[stream] += late
        self._align_forced_sum[stream] += forced
        self._last_buffer_len[stream] = buffer_len

        self._trim_align(stream)

    def on_event(self, stream: Stream, sanitization: SanitizationState, ev: Event) -> Tuple[DataTrustState, str]:
        is_q = 1 if sanitization == SanitizationState.QUARANTINE else 0
        self._san_quarantine_window[stream].append(is_q)
        self._san_quarantine_sum[stream] += is_q
        self._trim_san(stream)

        st, reason = self._eval_stream(stream, sanitization, ev)
        self._state_by_stream[stream] = st
        self._reason_by_stream[stream] = reason

        return self._reduce_global()

    def _eval_stream(self, stream: Stream, sanitization: SanitizationState, ev: Event) -> Tuple[DataTrustState, str]:
        untrusted_reasons = []
        degraded_reasons = []

        last_ts = self._last_event_ts.get(stream)
        last_id = self._last_event_id.get(stream)

        if last_ts is not None and stream in self._last_event_ts:
            cur_ts = last_ts
            if cur_ts is not None and last_ts is not None and cur_ts < last_ts:
                degraded_reasons.append("out_of_order_ts")

        if stream in self._last_event_id:
            if last_id is not None and last_id == self._last_event_id.get(stream):
                degraded_reasons.append("duplicate_event")

        san_total = len(self._san_quarantine_window[stream])
        q_rate = (self._san_quarantine_sum[stream] / san_total) if san_total else 0.0

        emitted = max(1, self._align_emitted_sum[stream])
        late_rate = self._align_late_sum[stream] / emitted
        forced_rate = self._align_forced_sum[stream] / emitted
        buf = self._last_buffer_len[stream]

        if q_rate >= self.quarantine_untrusted_rate:
            untrusted_reasons.append(f"q_rate={q_rate:.4f}>={self.quarantine_untrusted_rate}")

        if late_rate >= self.late_untrusted_rate:
            untrusted_reasons.append(f"late_rate={late_rate:.4f}>={self.late_untrusted_rate}")

        if forced_rate >= self.forced_flush_untrusted_rate:
            untrusted_reasons.append(f"forced_rate={forced_rate:.4f}>={self.forced_flush_untrusted_rate}")

        if buf >= self.buffer_len_untrusted:
            untrusted_reasons.append(f"buffer_len={buf}>={self.buffer_len_untrusted}")

        if sanitization == SanitizationState.QUARANTINE:
            degraded_reasons.append("quarantine")

        if late_rate >= self.late_degraded_rate:
            degraded_reasons.append(f"late_rate={late_rate:.4f}>={self.late_degraded_rate}")

        if forced_rate >= self.forced_flush_degraded_rate:
            degraded_reasons.append(f"forced_rate={forced_rate:.4f}>={self.forced_flush_degraded_rate}")

        if buf >= self.buffer_len_degraded:
            degraded_reasons.append(f"buffer_len={buf}>={self.buffer_len_degraded}")

        if stream == Stream.ORDERBOOK:
            top = self.lob_replayer.snapshot()
            if top:
                bid = top.best_bid
                ask = top.best_ask
                mid = top.mid

                if bid is not None and ask is not None:
                    if bid >= ask:
                        untrusted_reasons.append("crossed_market")

                    mid = (bid + ask) / 2
                    if mid > 0:
                        spread_bps = (ask - bid) / mid * 10_000
                        if spread_bps > self.spread_explode_bps:
                            degraded_reasons.append(f"spread_explode_bps={spread_bps:.1f}")

        if stream == Stream.TRADES:
            data = ev.data
            price = data.get("price")

            if price is not None:
                top = self.lob_replayer.snapshot()
                if top:
                    bid = top.best_bid
                    ask = top.best_ask
                    mid = top.mid
                    if bid is not None and ask is not None:
                        diff_bps = abs(price - mid) / mid * 10_000.0

                        if diff_bps >= self.fat_finger_untrusted_bps:
                            untrusted_reasons.append(f"fat_finger_mid_bps={diff_bps:.1f}")
                        elif diff_bps >= self.fat_finger_degraded_bps:
                            degraded_reasons.append(f"fat_finger_mid_bps={diff_bps:.1f}")

                if self._last_trade_price is not None and self._last_trade_price > 0:
                    jump_bps = abs(price - self._last_trade_price) / self._last_trade_price * 10_000.0
                    if jump_bps >= self.trade_jump_degraded_bps:
                        degraded_reasons.append(f"trade_jump_bps={jump_bps:.1f}")

                self._last_trade_price = price

        if untrusted_reasons:
            return DataTrustState.UNTRUSTED, " | ".join(untrusted_reasons)

        if degraded_reasons:
            return DataTrustState.DEGRADED, " | ".join(degraded_reasons)

        return DataTrustState.TRUSTED, ""

    def _reduce_global(self) -> Tuple[DataTrustState, str]:
        untrusted = []
        degraded = []

        for s, st in self._state_by_stream.items():
            r = self._reason_by_stream.get(s, "")
            if st == DataTrustState.UNTRUSTED:
                untrusted.append((s, r))
            elif st == DataTrustState.DEGRADED:
                degraded.append((s, r))

        if untrusted:
            reason = ", ".join(f"{s.value}:{r}" for s, r in untrusted if r)
            return DataTrustState.UNTRUSTED, reason or "untrusted"

        if degraded:
            reason = ", ".join(f"{s.value}:{r}" for s, r in degraded if r)
            return DataTrustState.DEGRADED, reason or "degraded"

        return DataTrustState.TRUSTED, ""

    def _trim_san(self, stream: Stream) -> None:
        w = self._san_quarantine_window[stream]
        while len(w) > self.window_events:
            self._san_quarantine_sum[stream] -= w.popleft()

    def _trim_align(self, stream: Stream) -> None:
        w = self._align_window[stream]
        while len(w) > self.window_events:
            emitted, late, forced, _ = w.popleft()
            self._align_emitted_sum[stream] -= emitted
            self._align_late_sum[stream] -= late
            self._align_forced_sum[stream] -= forced
