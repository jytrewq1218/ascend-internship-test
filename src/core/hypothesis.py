from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from src.adapters.base import Event, Stream
from src.core.types import HypothesisState


@dataclass
class _ConsensusResult:
    ok: bool
    worst_bps: Optional[float]
    worst_pair: Optional[str]
    sources: int


class HypothesisPolicy:
    def __init__(self, cfg: Dict[str, Any], lob_replayer):
        self.lob = lob_replayer

        self.weak_price_diverge_bps = cfg["weak_price_diverge_bps"]
        self.invalid_price_diverge_bps = cfg["invalid_price_diverge_bps"]

        self.stable_min_duration_us = cfg["stable_min_duration_ms"] * 1000
        self._stable_since_us: Optional[int] = None

        self.state: HypothesisState = HypothesisState.INVALID

        self.last_mark: Optional[float] = None
        self.last_index: Optional[float] = None
        self.last_last: Optional[float] = None

    def verify(self, ev: Event, now_us: int) -> Tuple[HypothesisState, str]:
        trigger_prefix = ""

        match ev.stream:
            case Stream.TICKER:
                data = ev.data
                mark_price = data["mark_price"]
                index_price = data["index_price"]
                last_price = data["last_price"]

                if mark_price is not None:
                    self.last_mark = mark_price
                if index_price is not None:
                    self.last_index = index_price
                if last_price is not None:
                    self.last_last = last_price

                trigger_prefix = "tickers"

            case Stream.ORDERBOOK:
                trigger_prefix = "orderbook"

            case Stream.LIQUIDATIONS:
                trigger_prefix = "liquidations"

            case Stream.TRADES:
                trigger_prefix = "trade"

        prices = self._collect_prices(ev)
        consensus = self._consensus(prices)

        if not consensus.ok:
            return self.state, f"{trigger_prefix}:no_change:insufficient_sources={consensus.sources}"

        worst = float(consensus.worst_bps or 0.0)

        if worst >= self.invalid_price_diverge_bps:
            self._stable_since_us = None
            self.state = HypothesisState.INVALID
            return self.state, f"{trigger_prefix}:worst_bps={worst:.1f} pair={consensus.worst_pair} sources={consensus.sources}"

        if worst >= self.weak_price_diverge_bps:
            self._stable_since_us = None
            self.state = HypothesisState.WEAKENING
            return self.state, f"{trigger_prefix}:worst_bps={worst:.1f} pair={consensus.worst_pair} sources={consensus.sources}"


        if self._stable_since_us is None:
            self._stable_since_us = now_us

        stable_duration = now_us - self._stable_since_us

        if stable_duration < self.stable_min_duration_us:
            return (
                self.state,
                f"{trigger_prefix}:stabilizing "
                f"elapsed_us={stable_duration} required_us={self.stable_min_duration_us}"
            )

        self.state = HypothesisState.VALID
        return self.state, f"{trigger_prefix}:stable_us={stable_duration}"

    def _collect_prices(self, ev: Event) -> Dict[str, float]:
        prices: Dict[str, float] = {}

        top = self.lob.snapshot()
        if top is not None:
            bid = top.best_bid
            ask = top.best_ask
            if bid is not None and ask is not None:
                prices["lob_mid"] = top.mid

        if self.last_mark is not None:
            prices["mark"] = self.last_mark
        if self.last_index is not None:
            prices["index"] = self.last_index
        if self.last_last is not None:
            prices["last"] = self.last_last

        if ev.stream in (Stream.TRADES, Stream.LIQUIDATIONS):
            price = ev.data["price"]
            if price is not None:
                prices[ev.stream.value] = price

        return prices

    def _consensus(self, prices: Dict[str, float]) -> _ConsensusResult:
        keys = [k for k, v in prices.items() if isinstance(v, (int, float)) and v > 0]
        if len(keys) < len(list(Stream)):
            return _ConsensusResult(ok=False, worst_bps=None, worst_pair=None, sources=len(keys))

        worst = -1.0
        worst_pair: Optional[str] = None

        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                price_i = prices[keys[i]]
                price_j = prices[keys[j]]
                bps = abs(price_i - price_j) / abs(price_j) * 10_000.0
                if bps > worst:
                    worst = bps
                    worst_pair = f"{keys[i]}~{keys[j]}"

        return _ConsensusResult(ok=True, worst_bps=worst, worst_pair=worst_pair, sources=len(keys))
