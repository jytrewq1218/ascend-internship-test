import json
import logging
import queue
import threading
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import websocket

from src.adapters.base import Adapter, Event, Stream
from src.utils.time import now_us, ms_to_us
from src.utils.parse import to_str, to_float, to_bool

logger = logging.getLogger(__name__)

WS_URL_BASE = "wss://fstream.binance.com/stream?streams="
REST_URL_BASE = "https://fapi.binance.com"


class BinanceWsAdapter(Adapter):
    def __init__(self, symbol: str, cfg: Dict[str, Any]):
        self.exchange = "binance-futures"
        self.symbol = symbol.lower()

        self.poll_interval_sec = cfg["poll_interval_ms"] / 1000.0

        self.open_interest_interval_sec = cfg["open_interest_interval_ms"] / 1000.0

        streams = [
            f"{self.symbol}@aggTrade",
            f"{self.symbol}@depth@100ms",
            f"{self.symbol}@forceOrder",
            f"{self.symbol}@markPrice@1s",
            f"{self.symbol}@ticker",
        ]
        self.ws_url = WS_URL_BASE + "/".join(streams)

        self.depth_snapshot_limit = int(cfg["depth_snapshot_limit"])
        self.rest_url_base = cfg.get("rest_url_base", REST_URL_BASE)

        self._stop: Optional[threading.Event] = None
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._oi_thread: Optional[threading.Thread] = None

        self._ticker_lock = threading.Lock()
        self._ticker_data: Dict[str, Any] = {
            "funding_timestamp": None,
            "funding_rate": None,
            "predicted_funding_rate": None,
            "open_interest": None,
            "last_price": None,
            "index_price": None,
            "mark_price": None,
            "ts_hour": None,
            "ts_minute": None,
        }

        logger.info(f"BinanceWsAdapter opened: ws_url={self.ws_url}, rest_url_base={self.rest_url_base}")

    def close(self) -> None:
        if self._stop is not None:
            self._stop.set()

        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                logger.exception("BinanceWsAdapter failed to close ws")

        if self._ws_thread is not None:
            try:
                self._ws_thread.join()
            except Exception:
                logger.exception("BinanceWsAdapter failed to close ws thread")
        
        if self._oi_thread is not None:
            try:
                self._oi_thread.join()
            except Exception:
                logger.exception("BinanceWsAdapter failed to close oi thread")

        logger.info("BinanceWsAdapter closed")

    def _fetch_snapshot_events(self) -> List[Event]:
        url = f"{self.rest_url_base}/fapi/v1/depth?symbol={self.symbol.upper()}&limit={self.depth_snapshot_limit}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req) as resp:
            ingest_ts = now_us()
            raw_data = json.loads(resp.read().decode("utf-8"))

        event_ts = ms_to_us(raw_data.get("E"))
        event_id = to_str(raw_data.get("lastUpdateId"))

        events: List[Event] = []

        for bid in raw_data.get("bids", []) or []:
            data = {
                "is_snapshot": True,
                "side": "bid",
                "price": to_float(bid[0]),
                "amount": to_float(bid[1]),
            }
            events.append(
                Event(
                    stream=Stream.ORDERBOOK,
                    exchange=self.exchange,
                    symbol=self.symbol.upper(),
                    event_ts=event_ts,
                    ingest_ts=ingest_ts,
                    event_id=event_id,
                    data=data,
                )
            )

        for ask in raw_data.get("asks", []) or []:
            data = {
                "is_snapshot": True,
                "side": "ask",
                "price": to_float(ask[0]),
                "amount": to_float(ask[1]),
            }
            events.append(
                Event(
                    stream=Stream.ORDERBOOK,
                    exchange=self.exchange,
                    symbol=self.symbol.upper(),
                    event_ts=event_ts,
                    ingest_ts=ingest_ts,
                    event_id=event_id,
                    data=data,
                )
            )

        return events

    def _poll_open_interest_loop(self, q: "queue.Queue[Event]", stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                url = f"{self.rest_url_base}/fapi/v1/openInterest?symbol={self.symbol.upper()}"
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    ingest_ts = now_us()
                    raw_data = json.loads(resp.read().decode("utf-8"))

                event_ts = ms_to_us(raw_data.get("time"))
                open_interest = to_float(raw_data.get("openInterest"))

                with self._ticker_lock:
                    self._ticker_data["open_interest"] = open_interest

                    if event_ts is not None:
                        dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                        self._ticker_data["ts_hour"] = dt.hour
                        self._ticker_data["ts_minute"] = dt.minute

                    snap = dict(self._ticker_data)

                q.put(
                    Event(
                        stream=Stream.TICKER,
                        exchange=self.exchange,
                        symbol=self.symbol.upper(),
                        event_ts=event_ts,
                        ingest_ts=ingest_ts,
                        event_id=None,
                        data=snap,
                    )
                )

            except Exception:
                logger.exception("Failed to fetch open interest")
            finally:
                time.sleep(self.open_interest_interval_sec)

    def stream_events(self) -> Iterator[Event]:
        q: queue.Queue[Event] = queue.Queue()
        stop = threading.Event()
        self._stop = stop

        try:
            snapshot_events = self._fetch_snapshot_events()
        except Exception as e:
            raise RuntimeError("Failed to fetch orderbook snapshot") from e

        for ev in snapshot_events:
            q.put(ev)

        oi_t = threading.Thread(
            target=self._poll_open_interest_loop,
            args=(q, stop),
            daemon=True,
        )
        self._oi_thread = oi_t
        oi_t.start()

        def on_open(ws):
            logger.info("WebSocket opened")

        def on_message(ws, msg: str):
            ingest_ts = now_us()
            try:
                raw = json.loads(msg)
                stream_name = raw.get("stream")
                data = raw.get("data")
                events = self._to_events(ingest_ts, stream_name, data)
                for ev in events:
                    q.put(ev)
            except Exception:
                logger.exception("WebSocket on_message failed")
                stop.set()

        def on_error(ws, err):
            logger.error(f"WebSocket error: {err}")
            stop.set()

        def on_close(ws, code, msg):
            logger.warning(f"WebSocket closed: {code} {msg}")
            stop.set()

        ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        self._ws = ws

        def _run_ws():
            try:
                ws.run_forever()
            except Exception:
                logger.exception("WS thread crashed with exception")
            finally:
                stop.set()

        t = threading.Thread(target=_run_ws, daemon=True)
        self._ws_thread = t
        t.start()

        try:
            while not stop.is_set():
                if self._ws_thread is not None and not self._ws_thread.is_alive():
                    stop.set()
                    break

                try:
                    yield q.get(timeout=self.poll_interval_sec)
                except queue.Empty:
                    continue
        finally:
            self.close()

    def _to_events(
        self,
        ingest_ts: int,
        stream_name: Optional[str],
        raw_data: Optional[Dict[str, Any]],
    ) -> List[Event]:
        if not stream_name or raw_data is None:
            return []

        event_ts = ms_to_us(raw_data.get("E")) or ingest_ts

        if stream_name == f"{self.symbol}@aggTrade":
            event_id = to_str(raw_data.get("a"))

            m = to_bool(raw_data.get("m"))
            if m is None:
                side = None
            elif m:
                side = "sell"
            else:
                side = "buy"

            ts_hour = ts_minute = ts_second = latency_us = None
            if event_ts is not None:
                dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                ts_hour, ts_minute, ts_second = dt.hour, dt.minute, dt.second
                latency_us = ingest_ts - event_ts

            return [
                Event(
                    stream=Stream.TRADES,
                    exchange=self.exchange,
                    symbol=self.symbol.upper(),
                    event_ts=event_ts,
                    ingest_ts=ingest_ts,
                    event_id=event_id,
                    data={
                        "side": side,
                        "price": to_float(raw_data.get("p")),
                        "amount": to_float(raw_data.get("q")),
                        "ts_hour": ts_hour,
                        "ts_minute": ts_minute,
                        "ts_second": ts_second,
                        "latency_us": latency_us,
                    },
                )
            ]

        if stream_name == f"{self.symbol}@depth@100ms":
            event_id = to_str(raw_data.get("u"))

            out: List[Event] = []
            for bid in raw_data.get("b", []) or []:
                px = bid[0] if len(bid) > 0 else None
                qty = bid[1] if len(bid) > 1 else None
                out.append(
                    Event(
                        stream=Stream.ORDERBOOK,
                        exchange=self.exchange,
                        symbol=self.symbol.upper(),
                        event_ts=event_ts,
                        ingest_ts=ingest_ts,
                        event_id=event_id,
                        data={
                            "is_snapshot": False,
                            "side": "bid",
                            "price": to_float(px),
                            "amount": to_float(qty),
                        },
                    )
                )
            for ask in raw_data.get("a", []) or []:
                px = ask[0] if len(ask) > 0 else None
                qty = ask[1] if len(ask) > 1 else None
                out.append(
                    Event(
                        stream=Stream.ORDERBOOK,
                        exchange=self.exchange,
                        symbol=self.symbol.upper(),
                        event_ts=event_ts,
                        ingest_ts=ingest_ts,
                        event_id=event_id,
                        data={
                            "is_snapshot": False,
                            "side": "ask",
                            "price": to_float(px),
                            "amount": to_float(qty),
                            "U": raw_data.get("U"),
                            "u": raw_data.get("u"),
                            "pu": raw_data.get("pu"),
                        },
                    )
                )
            return out

        if stream_name == f"{self.symbol}@forceOrder":
            o = raw_data.get("o") or {}

            side = to_str(o.get("S"))
            side = side.lower() if side is not None else None

            price = to_float(o.get("p"))
            amount = to_float(o.get("q"))
            event_id = to_str(o.get("i"))

            ts_hour = ts_minute = latency_us = None
            if event_ts is not None:
                dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                ts_hour, ts_minute = dt.hour, dt.minute
                latency_us = ingest_ts - event_ts

            return [
                Event(
                    stream=Stream.LIQUIDATIONS,
                    exchange=self.exchange,
                    symbol=self.symbol.upper(),
                    event_ts=event_ts,
                    ingest_ts=ingest_ts,
                    event_id=event_id,
                    data={
                        "side": side,
                        "price": price,
                        "amount": amount,
                        "ts_hour": ts_hour,
                        "ts_minute": ts_minute,
                        "latency_us": latency_us,
                    },
                )
            ]

        if stream_name == f"{self.symbol}@markPrice@1s":
            with self._ticker_lock:
                self._ticker_data["funding_timestamp"] = ms_to_us(raw_data.get("T"))
                self._ticker_data["funding_rate"] = to_float(raw_data.get("r"))
                self._ticker_data["index_price"] = to_float(raw_data.get("i"))
                self._ticker_data["mark_price"] = to_float(raw_data.get("p"))

                dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                self._ticker_data["ts_hour"] = dt.hour
                self._ticker_data["ts_minute"] = dt.minute

                snap = dict(self._ticker_data)

            return [
                Event(
                    stream=Stream.TICKER,
                    exchange=self.exchange,
                    symbol=self.symbol.upper(),
                    event_ts=event_ts,
                    ingest_ts=ingest_ts,
                    event_id=None,
                    data=snap,
                )
            ]

        if stream_name == f"{self.symbol}@ticker":
            with self._ticker_lock:
                self._ticker_data["last_price"] = to_float(raw_data.get("c"))

                dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                self._ticker_data["ts_hour"] = dt.hour
                self._ticker_data["ts_minute"] = dt.minute

                snap = dict(self._ticker_data)

            return [
                Event(
                    stream=Stream.TICKER,
                    exchange=self.exchange,
                    symbol=self.symbol.upper(),
                    event_ts=event_ts,
                    ingest_ts=ingest_ts,
                    event_id=None,
                    data=snap,
                )
            ]

        return []
