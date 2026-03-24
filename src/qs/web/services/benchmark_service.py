from __future__ import annotations

from collections import defaultdict

from qs.backtester.market import SqliteMarketData

from ..models.dto import BenchmarkPoint, EquityPoint


BENCHMARK_TABLES = {
    "000300.SH": "index_daily",
    "HSI": "index_global",
    "IXIC": "index_global",
}


class BenchmarkService:
    def __init__(self, market_data: SqliteMarketData):
        self.market_data = market_data

    def build_for_curve(
        self, equity_curve: list[EquityPoint], benchmark_codes: list[str]
    ) -> list[BenchmarkPoint]:
        if not equity_curve or not benchmark_codes:
            return []
        dates = [p.trade_date for p in equity_curve]
        out: list[BenchmarkPoint] = []
        for code in benchmark_codes:
            table = BENCHMARK_TABLES.get(code)
            if not table:
                continue
            px_map: dict[str, float] = {}
            for date in dates:
                row = self.market_data.get_price_map(
                    request=_benchmark_request(table),
                    symbols=[code],
                    trade_date=date,
                )
                px = row.get(code)
                if px is not None:
                    px_map[date] = px
            if not px_map:
                continue
            first = next(iter(px_map.values()))
            if first <= 0:
                continue
            for date in dates:
                px = px_map.get(date)
                if px is None:
                    continue
                out.append(BenchmarkPoint(benchmark_code=code, trade_date=date, nav=px / first))
        return out

    @staticmethod
    def regroup(points: list[BenchmarkPoint]) -> dict[str, list[dict[str, float | str]]]:
        grouped: dict[str, list[dict[str, float | str]]] = defaultdict(list)
        for point in points:
            grouped[point.benchmark_code].append(
                {"trade_date": point.trade_date, "nav": point.nav}
            )
        return dict(grouped)


def _benchmark_request(table: str):
    from qs.backtester.market import PriceRequest

    return PriceRequest(table=table, field="close", adjusted=False, exact=False)
