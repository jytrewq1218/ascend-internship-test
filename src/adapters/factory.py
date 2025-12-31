from pathlib import Path
from typing import Any, Dict

from src.adapters.csv_adapter import CsvAdapter
from src.adapters.binance_ws_adapter import BinanceWsAdapter


def build_adapter(cfg: Dict[str, Any], mode: str):
    match mode:
        case "historical":
            data_dir = Path(cfg["paths"]["data_root"]) / cfg["paths"]["phase"]
            return CsvAdapter(data_dir, cfg["adapters"]["csv"])
        case "realtime":
            exchange = cfg["exchange"]
            match exchange:
                case "binance-futures":
                    return BinanceWsAdapter(cfg["symbol"], cfg["adapters"]["ws"])
                case _:
                    raise ValueError(f"Unsupported exchange: {exchange}")
        case _:
            raise ValueError(f"Unsupported mode: {mode}")
