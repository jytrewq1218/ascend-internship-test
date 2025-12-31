from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, Optional
from abc import abstractmethod


class Stream(str, Enum):
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    LIQUIDATIONS = "liquidations"
    TICKER = "ticker"


@dataclass(frozen=True)
class Event:
    stream: Stream
    exchange: Optional[str]
    symbol: Optional[str]
    event_ts: int
    ingest_ts: int
    event_id: Optional[str]
    data: Dict[str, Any]

    def __str__(self) -> str:
        core_str = (
            f"stream={self.stream.value} "
            f"exchange={self.exchange} "
            f"symbol={self.symbol} "
            f"event_ts={self.event_ts} "
            f"ingest_ts={self.ingest_ts}"
        )
        if self.event_id is not None:
            core_str += f" event_id={self.event_id}"

        data_str = ", ".join(f"{k}={v}" for k, v in self.data.items())
        return f"Event({core_str} | data={ {data_str} })"


class Adapter:
    @abstractmethod
    def stream_events(self) -> Iterator[Event]:
        pass

    def close(self) -> None:
        return
