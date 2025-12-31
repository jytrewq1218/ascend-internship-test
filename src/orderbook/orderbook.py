from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class BookTop:
    best_bid: Optional[float]
    best_ask: Optional[float]
    mid: Optional[float]
    spread: Optional[float]


def _best_from_levels(levels: Dict[float, float], side: str) -> Optional[Tuple[float, float]]:
    if not levels:
        return None
    px = max(levels.keys()) if side == "bid" else min(levels.keys())
    return px, levels[px]


class OrderBook:
    def __init__(self, depth_limit: int) -> None:
        self.depth_limit = depth_limit

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

        self.last_update_us: Optional[int] = None
        self.last_event_ts: Optional[int] = None

    def _trim(self) -> None:
        if self.depth_limit <= 0:
            return

        if len(self.bids) > self.depth_limit:
            for px in sorted(self.bids.keys())[: len(self.bids) - self.depth_limit]:
                self.bids.pop(px, None)

        if len(self.asks) > self.depth_limit:
            for px in sorted(self.asks.keys(), reverse=True)[: len(self.asks) - self.depth_limit]:
                self.asks.pop(px, None)

    def apply_snapshot(self, side: str, price: float, amount: float, now_us: int, event_ts: Optional[int] = None) -> None:
        if side == "bid":
            self.bids[price] = amount
        else:
            self.asks[price] = amount

        self._trim()
        self.last_update_us = now_us
        if event_ts is not None:
            self.last_event_ts = event_ts

    def apply_delta(self, side: str, price: float, amount: float, now_us: int, event_ts: Optional[int] = None) -> None:
        book = self.bids if side == "bid" else self.asks
        if amount <= 0:
            book.pop(price, None)
        else:
            book[price] = amount

        self._trim()
        self.last_update_us = now_us
        if event_ts is not None:
            self.last_event_ts = event_ts

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def top(self) -> BookTop:
        bb = _best_from_levels(self.bids, "bid")
        ba = _best_from_levels(self.asks, "ask")
        best_bid = bb[0] if bb else None
        best_ask = ba[0] if ba else None

        if best_bid is None or best_ask is None:
            return BookTop(best_bid, best_ask, None, None)

        mid = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        return BookTop(best_bid, best_ask, mid, spread)
