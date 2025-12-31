import logging
import threading
import time
from typing import Optional
from typing import Any, Callable, Dict, Optional

from src.core.engine import SingleDecisionEngine
from src.utils.time import now_us


def start_tick_loop(
    engine: SingleDecisionEngine,
    interval_sec: float,
    stop_event: threading.Event,
    logger: logging.Logger,
) -> Optional[threading.Thread]:
    def tick_loop() -> None:
        while not stop_event.is_set():
            try:
                engine.tick(now_us())
            except Exception:
                logger.exception("Failed to start tick loop")
            time.sleep(interval_sec)

    t = threading.Thread(target=tick_loop, daemon=True)
    t.start()

    logger.info(f"Tick loop started")
    return t


def run_loop(
    mode: str,
    cfg: Dict[str, Any],
    engine: SingleDecisionEngine,
    build_adapter: Callable[[Dict[str, Any], str], Any],
    logger: logging.Logger,
) -> None:
    logger.info(f"Main loop started")

    reconnect_delay_sec = cfg["adapters"]["ws"]["reconnect_delay_ms"] / 1000.0

    while True:
        adapter = None
        try:
            adapter = build_adapter(cfg, mode)

            for event in adapter.stream_events():
                engine.ingest(event)

            if mode == "historical":
                logger.info("Historical adapter replay completed; keeping engine")
                while True:
                    time.sleep(1.0)
            else:
                logger.warning(f"Realtime adapter terminated; restarting in {reconnect_delay_sec:.3f}s")
                time.sleep(reconnect_delay_sec)

        except Exception:
            logger.exception(f"Adapter error; restarting in {reconnect_delay_sec:.3f}s")
            time.sleep(reconnect_delay_sec)

        finally:
            if adapter is not None:
                adapter.close()
