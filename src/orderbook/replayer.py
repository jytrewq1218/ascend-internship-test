from typing import Any, Dict, Optional, Tuple

from src.adapters.base import Event, Stream
from src.orderbook.orderbook import OrderBook, BookTop

class OrderBookReplayer:
    def __init__(self, depth_limit: int) -> None:
        self.orderbook = OrderBook(depth_limit=depth_limit)
        self._snapshot_active = False

    def on_event(self, ev: Event, now_us: int) -> None:
        if ev.stream != Stream.ORDERBOOK:
            return None

        data = ev.data
        is_snapshot = data["is_snapshot"]
        side = data["side"]
        price = data["price"]
        amount = data["amount"]

        if side not in ("bid", "ask"):
            return None
        if not isinstance(price, (int, float)) or not isinstance(amount, (int, float)):
            return None

        if is_snapshot:
            if not self._snapshot_active:
                self.orderbook.clear()
                self._snapshot_active = True
            self.orderbook.apply_snapshot(side, float(price), float(amount), now_us, ev.event_ts)
        else:
            self._snapshot_active = False
            self.orderbook.apply_delta(side, float(price), float(amount), now_us, ev.event_ts)
        return None

    def snapshot(self) -> BookTop:
        return self.orderbook.top()