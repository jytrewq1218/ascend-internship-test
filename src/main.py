import logging
import sys
import threading
from pathlib import Path

from src.config.load_cfg import load_cfg
from src.adapters.factory import build_adapter
from src.core.engine import SingleDecisionEngine
from src.utils.logger import set_logger
from src.utils.output_writer import OutputWriter
from src.runtime.runner import start_tick_loop, run_loop


def main() -> None:
    if len(sys.argv) != 2:
        raise RuntimeError(
            "Phase 1: docker run -v /path/to/data:/data <image> historical\n"
            "Phase 2: docker run <image> realtime"
        )

    mode = sys.argv[1]
    cfg = load_cfg(mode)

    log_dir = Path(cfg["paths"]["log_root"]) / mode
    set_logger(log_dir)

    logger = logging.getLogger(__name__)
    logger.info(" Initializing ".center(50, "="))
    logger.info(f"Logger initialized: log_dir={log_dir}")

    output_dir = Path(cfg["paths"]["output_root"]) / mode
    writer = OutputWriter(output_dir)
    logger.info(f"OutputWriter initialized: output_dir={output_dir}")

    engine = SingleDecisionEngine(cfg, writer)

    try:
        logger.info(" Starting ".center(50, "="))

        tick_interval_sec = cfg["engine"]["tick_interval_ms"] / 1000.0
        stop_event = threading.Event()
        start_tick_loop(
            engine=engine,
            interval_sec=tick_interval_sec,
            stop_event=stop_event,
            logger=logger
        )

        run_loop(
            mode=mode,
            cfg=cfg,
            engine=engine,
            build_adapter=build_adapter,
            logger=logger,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    finally:
        logger.info(" Shutting down ".center(50, "="))

        stop_event.set()
        logger.info(f"Tick loop stopped")

        engine.shutdown()

        writer.finalize()
        logger.info(f"OutputWriter saved outputs")


if __name__ == "__main__":
    main()
