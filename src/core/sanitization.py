from typing import Any, Dict, Tuple, Optional

from src.adapters.base import Event, Stream
from src.core.types import SanitizationState


class Sanitizer:
    def __init__(
        self,
        cfg: Dict[str, Any],
        exchange: str,
        symbol: str,
    ):
        self._exchange = exchange
        self._symbol = symbol

        self._last_funding_timestamp: Optional[int] = None
        self._last_funding_rate: Optional[float] = None
        self._last_predicted_funding_rate: Optional[float] = None
        self._last_open_interest: Optional[float] = None
        self._last_last_price: Optional[float] = None
        self._last_index_price: Optional[float] = None
        self._last_mark_price: Optional[float] = None

    def sanitize(self, ev: Event) -> Tuple[SanitizationState, Event, str]:
        exchange = ev.exchange
        symbol = ev.symbol

        status = SanitizationState.ACCEPT
        reasons = []

        if exchange is None:
            exchange = self._exchange
            status = SanitizationState.REPAIR
            reason.append("repair_exchange_default")
        elif exchange != self._exchange:
            return SanitizationState.QUARANTINE, ev, "missing_exchange"

        if symbol is None:
            if self._symbol:
                symbol = self._symbol
                status = SanitizationState.REPAIR
                reasons.append("repair_symbol_default")
        elif symbol != self._symbol:
            return SanitizationState.QUARANTINE, ev, "missing_symbol"

        data = ev.data
        match ev.stream:
            case Stream.TRADES:
                if data.get("price") is None or data.get("amount") is None or data.get("side") is None:
                    return SanitizationState.QUARANTINE, ev, "trade_missing_fields"

            case Stream.LIQUIDATIONS:
                if data.get("price") is None or data.get("amount") is None or data.get("side") is None:
                    return SanitizationState.QUARANTINE, ev, "liq_missing_fields"

            case Stream.ORDERBOOK:
                if "is_snapshot" in data and data.get("is_snapshot") is None:
                    return SanitizationState.QUARANTINE, ev, "orderbook_invalid_is_snapshot"

                if data.get("side") is None or data.get("price") is None or data.get("amount") is None:
                    return SanitizationState.QUARANTINE, ev, "orderbook_missing_fields"

            case Stream.TICKER:
                merged = dict(data)

                required = [
                    "funding_timestamp",
                    "funding_rate",
                    "open_interest",
                    "last_price",
                    "index_price",
                    "mark_price",
                ]
                complete_in_payload = all(merged.get(k) is not None for k in required)

                if merged.get("funding_timestamp") is not None:
                    self._last_funding_timestamp = merged["funding_timestamp"]
                if merged.get("funding_rate") is not None:
                    self._last_funding_rate = merged["funding_rate"]
                if merged.get("predicted_funding_rate") is not None:
                    self._last_predicted_funding_rate = merged["predicted_funding_rate"]
                if merged.get("open_interest") is not None:
                    self._last_open_interest = merged["open_interest"]
                if merged.get("last_price") is not None:
                    self._last_last_price = merged["last_price"]
                if merged.get("index_price") is not None:
                    self._last_index_price = merged["index_price"]
                if merged.get("mark_price") is not None:
                    self._last_mark_price = merged["mark_price"]

                used_cache = False

                def fill(key: str, cached):
                    nonlocal used_cache
                    if merged.get(key) is None and cached is not None:
                        merged[key] = cached
                        used_cache = True

                fill("funding_timestamp", self._last_funding_timestamp)
                fill("funding_rate", self._last_funding_rate)
                fill("predicted_funding_rate", self._last_predicted_funding_rate)
                fill("open_interest", self._last_open_interest)
                fill("last_price", self._last_last_price)
                fill("index_price", self._last_index_price)
                fill("mark_price", self._last_mark_price)

                missing = [k for k in required if merged.get(k) is None]
                if missing:
                    return SanitizationState.QUARANTINE, ev, f"ticker_missing_fields:{','.join(missing)}"

                if complete_in_payload and not used_cache:
                    if status == SanitizationState.REPAIR:
                        pass
                    else:
                        status = SanitizationState.ACCEPT
                else:
                    status = SanitizationState.REPAIR
                    data = merged
                    reasons.append("repair_ticker_merge_cache")

            case _:
                return SanitizationState.QUARANTINE, ev, "unknown_stream"

        reason = "|".join(reasons) if reasons else ""
        if status == SanitizationState.REPAIR:
            repaired_ev = Event(
                stream=ev.stream,
                exchange=exchange,
                symbol=symbol,
                event_ts=ev.event_ts,
                ingest_ts=ev.ingest_ts,
                event_id=ev.event_id,
                data=dict(data),
            )
            return status, repaired_ev, reason

        return status, ev, reason
