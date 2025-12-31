import heapq
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.adapters.base import Event


@dataclass
class TimeAlignmentStats:
    pushed: int = 0
    emitted: int = 0
    late: int = 0
    forced_flush: bool = False
    buffer_len: int = 0


class TimeAligner:
    def __init__(self, cfg: Dict[str, Any]):
        self.allowed_lateness_us = cfg["allowed_lateness_ms"] * 1000
        self.max_buffer_us = cfg["max_buffer_ms"] * 1000

        self._last_event_ts: Optional[int] = None
        self._heap: List[Tuple[int, int, Event]] = []
        self._tie = 0

    def align(self, ev: Event) -> Tuple[List[Event], TimeAlignmentStats]:
        stats = TimeAlignmentStats(pushed=1)

        event_ts = ev.event_ts
        if event_ts is None:
            stats.emitted = 1
            stats.buffer_len = len(self._heap)
            return [ev], stats

        if self._last_event_ts is None or event_ts > self._last_event_ts:
            self._last_event_ts = event_ts

        prev_watermark = self._compute_watermark()
        if prev_watermark is not None and event_ts < prev_watermark:
            stats.late = 1

        heapq.heappush(self._heap, (event_ts, self._tie, ev))
        self._tie += 1

        aligned_evs = []
        watermark = self._compute_watermark()
        if watermark is None:
            stats.buffer_len = len(self._heap)
            return aligned_evs, stats

        if self._heap:
            first_event_ts = self._heap[0][0]
            if (watermark - first_event_ts) > self.max_buffer_us:
                watermark = first_event_ts + self.max_buffer_us
                stats.forced_flush = True

        while self._heap and self._heap[0][0] <= watermark:
            _, _, e = heapq.heappop(self._heap)
            aligned_evs.append(e)

        stats.emitted = len(aligned_evs)
        stats.buffer_len = len(self._heap)
        return aligned_evs, stats

    def _compute_watermark(self) -> Optional[int]:
        if self._last_event_ts is None:
            return None
        return self._last_event_ts - self.allowed_lateness_us
