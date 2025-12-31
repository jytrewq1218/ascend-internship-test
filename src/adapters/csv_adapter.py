import csv
import gzip
import heapq
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Tuple

from src.adapters.base import Adapter, Event, Stream
from src.utils.parse import to_str, to_int, to_float, to_bool

logger = logging.getLogger(__name__)


def open_file(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, mode="rt", encoding="utf-8", newline="")
    if path.suffix == ".csv":
        return open(path, mode="r", encoding="utf-8", newline="")
    raise ValueError(f"Unsupported file suffix: {path}")


class CsvAdapter(Adapter):
    def __init__(self, data_dir: Path, cfg: Dict[str, Any]):
        self.data_dir = Path(data_dir)
        self.replay_speed = float(cfg.get("replay_speed", 0))
        self.max_replay_sleep_sec = float(cfg.get("max_replay_sleep_ms", 5000)) / 1000.0

        self.paths = {
            Stream.TRADES: self._find_file(Stream.TRADES),
            Stream.ORDERBOOK: self._find_file(Stream.ORDERBOOK),
            Stream.LIQUIDATIONS: self._find_file(Stream.LIQUIDATIONS),
            Stream.TICKER: self._find_file(Stream.TICKER),
        }

        logger.info(f"CsvAdapter initialized: data_dir={self.data_dir}")

    def _find_file(self, stream: Stream) -> Path:
        name = stream.value
        gz_path = self.data_dir / f"{name}.csv.gz"
        csv_path = self.data_dir / f"{name}.csv"
        if gz_path.exists():
            return gz_path
        if csv_path.exists():
            return csv_path
        raise FileNotFoundError(f"Missing {name}.csv(.gz) under {self.data_dir}")

    def _iter_csv(self, stream: Stream, path: Path) -> Iterator[Event]:
        with open_file(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ev = self._row_to_event(stream, row)
                if ev is not None:
                    yield ev

    def _row_to_event(self, stream: Stream, row: Dict[str, str]) -> Optional[Event]:
        exchange = to_str(row.get("exchange"))
        symbol = to_str(row.get("symbol"))
        event_ts = to_int(row.get("timestamp"))
        ingest_ts = to_int(row.get("local_timestamp"))
        event_id = to_str(row.get("id"))

        data: Dict[str, Any] = dict(row)

        match stream:
            case Stream.TRADES:
                ts_hour = ts_minute = ts_second = latency_us = None
                if event_ts is not None:
                    dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                    ts_hour, ts_minute, ts_second = dt.hour, dt.minute, dt.second
                    latency_us = ingest_ts - event_ts

                data.update(
                    {
                        "side": to_str(row.get("side")),
                        "price": to_float(row.get("price")),
                        "amount": to_float(row.get("amount")),
                        "ts_hour": ts_hour,
                        "ts_minute": ts_minute,
                        "ts_second": ts_second,
                        "latency_us": latency_us,
                    }
                )

            case Stream.ORDERBOOK:
                data.update(
                    {
                        "is_snapshot": to_bool(row.get("is_snapshot")),
                        "side": to_str(row.get("side")),
                        "price": to_float(row.get("price")),
                        "amount": to_float(row.get("amount")),
                    }
                )

            case Stream.LIQUIDATIONS:
                ts_hour = ts_minute = latency_us = None
                if event_ts is not None:
                    dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                    ts_hour, ts_minute = dt.hour, dt.minute
                    latency_us = ingest_ts - event_ts

                data.update(
                    {
                        "side": to_str(row.get("side")),
                        "price": to_float(row.get("price")),
                        "amount": to_float(row.get("amount")),
                        "ts_hour": ts_hour,
                        "ts_minute": ts_minute,
                        "latency_us": latency_us,
                    }
                )

            case Stream.TICKER:
                ts_hour = ts_minute = ts_second = latency_us = None
                if event_ts is not None:
                    dt = datetime.fromtimestamp(event_ts / 1_000_000, tz=timezone.utc)
                    ts_hour, ts_minute, ts_second = dt.hour, dt.minute, dt.second
                    latency_us = ingest_ts - event_ts

                data.update(
                    {
                        "funding_timestamp": to_int(row.get("funding_timestamp")),
                        "funding_rate": to_float(row.get("funding_rate")),
                        "predicted_funding_rate": to_float(row.get("predicted_funding_rate")),
                        "open_interest": to_float(row.get("open_interest")),
                        "last_price": to_float(row.get("last_price")),
                        "index_price": to_float(row.get("index_price")),
                        "mark_price": to_float(row.get("mark_price")),
                        "ts_hour": ts_hour,
                        "ts_minute": ts_minute,
                        "ts_second": ts_second,
                        "latency_us": latency_us,
                    }
                )

        return Event(
            stream=stream,
            exchange=exchange,
            symbol=symbol,
            event_ts=event_ts,
            ingest_ts=ingest_ts,
            event_id=event_id,
            data=data,
        )

    def stream_events(self) -> Iterator[Event]:
        heap: list[Tuple[int, int, Stream, Event, Iterator[Event]]] = []
        tie = 0

        for stream, path in self.paths.items():
            it = self._iter_csv(stream, path)
            first = next(it, None)
            if first is not None and first.ingest_ts is not None:
                heapq.heappush(heap, (first.ingest_ts, tie, stream, first, it))
                tie += 1

        prev_ingest: Optional[int] = None

        while heap:
            ingest_ts, _, stream, ev, it = heapq.heappop(heap)

            if self.replay_speed > 0 and prev_ingest is not None:
                delta = ingest_ts - prev_ingest
                if delta > 0:
                    sleep_sec = (delta / 1_000_000.0) * self.replay_speed
                    sleep_sec = min(sleep_sec, self.max_replay_sleep_sec)
                    time.sleep(sleep_sec)

            yield ev
            prev_ingest = ingest_ts

            nxt = next(it, None)
            if nxt is not None and nxt.ingest_ts is not None:
                heapq.heappush(heap, (nxt.ingest_ts, tie, stream, nxt, it))
                tie += 1
