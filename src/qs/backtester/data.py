from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Bar:
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    pct_chg: Optional[float]


class DataFeed:
    def __init__(self, bars: List[Bar]):
        self._bars = bars
        self._i = 0

    def __len__(self):
        return len(self._bars)

    @property
    def idx(self) -> int:
        return self._i

    @property
    def current(self) -> Bar:
        return self._bars[self._i]

    @property
    def prev(self) -> Optional[Bar]:
        if self._i == 0:
            return None
        return self._bars[self._i - 1]

    def step(self) -> bool:
        if self._i + 1 >= len(self._bars):
            return False
        self._i += 1
        return True

    def reset(self):
        self._i = 0
