from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from qs.sqlite_utils import connect_sqlite

from .broker import Broker


@dataclass(frozen=True)
class PriceRequest:
    table: str
    field: str
    adjusted: bool = False
    adjustment_table: str | None = None
    exact: bool = True


class SqliteMarketData:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._con = connect_sqlite(self.db_path, read_only=True)
        self._base_adj_cache: Dict[tuple[str, str], float] = {}

    def close(self) -> None:
        self._con.close()

    def history(self, as_of_date: str | None) -> HistoricalMarketView:
        return HistoricalMarketView(self, as_of_date)

    def reference(self) -> ReferenceDataView:
        return ReferenceDataView(self)

    def get_dataset_values(
        self,
        *,
        table: str,
        symbols: Sequence[str],
        fields: Sequence[str],
        trade_date: str,
        exact: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
        if not syms:
            return {}
        if not fields:
            raise ValueError("fields must not be empty")
        cols = ", ".join([f'd."{f}"' for f in fields])
        in_list = ",".join([repr(s) for s in syms])
        if exact:
            sql = f"""
            SELECT d.ts_code, {cols}
            FROM "{table}" d
            WHERE d.trade_date=? AND d.ts_code IN ({in_list})
            """
            rows = self._con.execute(sql, [trade_date]).fetchall()
        else:
            sql = f"""
            WITH last AS (
              SELECT ts_code, MAX(trade_date) AS trade_date
              FROM "{table}"
              WHERE ts_code IN ({in_list}) AND trade_date <= ?
              GROUP BY ts_code
            )
            SELECT d.ts_code, {cols}
            FROM last l
            JOIN "{table}" d
              ON d.ts_code=l.ts_code AND d.trade_date=l.trade_date
            """
            rows = self._con.execute(sql, [trade_date]).fetchall()

        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            ts_code = str(row[0])
            out[ts_code] = {field: row[idx + 1] for idx, field in enumerate(fields)}
        return out

    def get_latest_trade_date(self, *, table: str, on_or_before: str) -> str | None:
        row = self._con.execute(
            f'SELECT MAX(trade_date) FROM "{table}" WHERE trade_date <= ?',
            [on_or_before],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0])

    def get_snapshot_rows(
        self,
        *,
        table: str,
        fields: Sequence[str],
        trade_date: str,
        exact: bool = True,
        symbols: Sequence[str] | None = None,
    ) -> list[Dict[str, Any]]:
        if not fields:
            raise ValueError("fields must not be empty")
        symbol_sql = ""
        params: list[Any] = [trade_date]
        if symbols is not None:
            syms = [str(s).strip() for s in symbols if str(s).strip()]
            if not syms:
                return []
            symbol_sql = f' AND d.ts_code IN ({",".join([repr(s) for s in syms])})'
        cols = ", ".join([f'd."{f}"' for f in fields])
        if exact:
            sql = f"""
            SELECT d.ts_code, d.trade_date, {cols}
            FROM "{table}" d
            WHERE d.trade_date=?{symbol_sql}
            ORDER BY d.ts_code
            """
        else:
            inner_symbol_sql = ""
            if symbols is not None:
                syms = [str(s).strip() for s in symbols if str(s).strip()]
                inner_symbol_sql = f' AND ts_code IN ({",".join([repr(s) for s in syms])})'
            sql = f"""
            WITH last AS (
              SELECT ts_code, MAX(trade_date) AS trade_date
              FROM "{table}"
              WHERE trade_date <= ?{inner_symbol_sql}
              GROUP BY ts_code
            )
            SELECT d.ts_code, d.trade_date, {cols}
            FROM last l
            JOIN "{table}" d
              ON d.ts_code=l.ts_code AND d.trade_date=l.trade_date
            ORDER BY d.ts_code
            """
        rows = self._con.execute(sql, params).fetchall()
        out: list[Dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "ts_code": str(row[0]),
                    "trade_date": str(row[1]),
                    **{field: row[idx + 2] for idx, field in enumerate(fields)},
                }
            )
        return out

    def get_price_map(
        self,
        *,
        request: PriceRequest,
        symbols: Sequence[str],
        trade_date: str,
    ) -> Dict[str, float]:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
        if not syms:
            return {}
        in_list = ",".join([repr(s) for s in syms])
        if request.adjusted and not request.adjustment_table:
            raise ValueError("adjustment_table is required when adjusted=True")

        joins = ""
        select_adj = ""
        if request.adjusted and request.adjustment_table:
            joins = (
                f'LEFT JOIN "{request.adjustment_table}" af '
                'ON af.ts_code=d.ts_code AND af.trade_date=d.trade_date'
            )
            select_adj = ', COALESCE(af.adj_factor, 1.0) AS adj_factor'

        if request.exact:
            sql = f"""
            SELECT d.ts_code, d.trade_date, d."{request.field}" AS px{select_adj}
            FROM "{request.table}" d
            {joins}
            WHERE d.trade_date=? AND d.ts_code IN ({in_list})
            """
            rows = self._con.execute(sql, [trade_date]).fetchall()
        else:
            sql = f"""
            WITH last AS (
              SELECT ts_code, MAX(trade_date) AS trade_date
              FROM "{request.table}"
              WHERE ts_code IN ({in_list}) AND trade_date <= ?
              GROUP BY ts_code
            )
            SELECT d.ts_code, d.trade_date, d."{request.field}" AS px{select_adj}
            FROM last l
            JOIN "{request.table}" d
              ON d.ts_code=l.ts_code AND d.trade_date=l.trade_date
            {joins}
            """
            rows = self._con.execute(sql, [trade_date]).fetchall()

        out: Dict[str, float] = {}
        for row in rows:
            ts_code = str(row[0])
            px = row[2]
            if px is None:
                continue
            value = float(px)
            if value <= 0:
                continue
            if request.adjusted and request.adjustment_table:
                adj_factor = float(row[3] if row[3] is not None else 1.0)
                base = self._load_base_adj(request.adjustment_table, ts_code)
                if base == 0:
                    base = 1.0
                value = value * adj_factor / base
            out[ts_code] = value
        return out

    def _load_base_adj(self, table: str, symbol: str) -> float:
        key = (table, symbol)
        cached = self._base_adj_cache.get(key)
        if cached is not None:
            return cached
        row = self._con.execute(
            f"""
            SELECT adj_factor
            FROM "{table}"
            WHERE ts_code=?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            [symbol],
        ).fetchone()
        base = float(row[0]) if row and row[0] is not None else 1.0
        self._base_adj_cache[key] = base
        return base

    def get_reference_values(
        self,
        *,
        table: str,
        symbols: Sequence[str] | None,
        fields: Sequence[str],
    ) -> Dict[str, Dict[str, Any]]:
        if not fields:
            raise ValueError("fields must not be empty")
        cols = ", ".join([f'"{f}"' for f in fields])
        sql = f'SELECT ts_code, {cols} FROM "{table}"'
        if symbols is not None:
            syms = [str(s).strip() for s in symbols if str(s).strip()]
            if not syms:
                return {}
            in_list = ",".join([repr(s) for s in syms])
            sql += f" WHERE ts_code IN ({in_list})"
        rows = self._con.execute(sql).fetchall()
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            ts_code = str(row[0])
            out[ts_code] = {field: row[idx + 1] for idx, field in enumerate(fields)}
        return out

    def get_hk_to_cny_rate(self, trade_date: str) -> float | None:
        row = self._con.execute(
            """
            WITH d AS (
              SELECT trade_date
              FROM fx_daily
              WHERE ts_code IN ('USDCNH.FXCM','USDHKD.FXCM') AND trade_date <= ?
              GROUP BY trade_date
              HAVING COUNT(DISTINCT ts_code)=2
              ORDER BY trade_date DESC
              LIMIT 1
            )
            SELECT
              MAX(CASE WHEN f.ts_code='USDCNH.FXCM' THEN (f.bid_close+f.ask_close)/2 END) AS usd_cnh_mid,
              MAX(CASE WHEN f.ts_code='USDHKD.FXCM' THEN (f.bid_close+f.ask_close)/2 END) AS usd_hkd_mid
            FROM d
            JOIN fx_daily f
              ON f.trade_date=d.trade_date AND f.ts_code IN ('USDCNH.FXCM','USDHKD.FXCM')
            """,
            [trade_date],
        ).fetchone()
        if not row:
            return None
        usd_cnh, usd_hkd = row
        if usd_cnh is None or usd_hkd is None or float(usd_hkd) == 0.0:
            return None
        return float(usd_cnh) / float(usd_hkd)


class HistoricalMarketView:
    def __init__(self, market_data: SqliteMarketData, as_of_date: str | None):
        self._market_data = market_data
        self.as_of_date = as_of_date

    def get_dataset_values(
        self,
        *,
        table: str,
        symbols: Sequence[str],
        fields: Sequence[str],
        trade_date: str | None = None,
        exact: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        use_date = self._resolve_date(trade_date)
        if use_date is None:
            return {}
        return self._market_data.get_dataset_values(
            table=table,
            symbols=symbols,
            fields=fields,
            trade_date=use_date,
            exact=exact,
        )

    def get_price_map(
        self,
        *,
        request: PriceRequest,
        symbols: Sequence[str],
        trade_date: str | None = None,
    ) -> Dict[str, float]:
        use_date = self._resolve_date(trade_date)
        if use_date is None:
            return {}
        return self._market_data.get_price_map(
            request=request,
            symbols=symbols,
            trade_date=use_date,
        )

    def get_snapshot_rows(
        self,
        *,
        table: str,
        fields: Sequence[str],
        trade_date: str | None = None,
        exact: bool = True,
        symbols: Sequence[str] | None = None,
    ) -> list[Dict[str, Any]]:
        use_date = self._resolve_date(trade_date)
        if use_date is None:
            return []
        return self._market_data.get_snapshot_rows(
            table=table,
            fields=fields,
            trade_date=use_date,
            exact=exact,
            symbols=symbols,
        )

    def _resolve_date(self, trade_date: str | None) -> str | None:
        if self.as_of_date is None:
            if trade_date is not None:
                raise ValueError("no historical data is available before the first bar")
            return None
        use_date = trade_date or self.as_of_date
        if use_date > self.as_of_date:
            raise ValueError(
                f"requested date {use_date} exceeds historical cutoff {self.as_of_date}"
            )
        return use_date

    def get_hk_to_cny_rate(self, trade_date: str | None = None) -> float | None:
        use_date = self._resolve_date(trade_date)
        if use_date is None:
            return None
        return self._market_data.get_hk_to_cny_rate(use_date)

    def get_latest_trade_date(self, *, table: str, on_or_before: str | None = None) -> str | None:
        use_date = self._resolve_date(on_or_before)
        if use_date is None:
            return None
        return self._market_data.get_latest_trade_date(
            table=table,
            on_or_before=use_date,
        )


class ReferenceDataView:
    def __init__(self, market_data: SqliteMarketData):
        self._market_data = market_data

    def get_values(
        self,
        *,
        table: str,
        symbols: Sequence[str] | None,
        fields: Sequence[str],
    ) -> Dict[str, Dict[str, Any]]:
        return self._market_data.get_reference_values(
            table=table,
            symbols=symbols,
            fields=fields,
        )


class PortfolioView:
    def __init__(self, broker: Broker):
        self._positions = {
            sym: float(pos.size)
            for sym, pos in broker.positions.items()
            if pos.size and pos.size > 0
        }

    @property
    def positions(self) -> Mapping[str, float]:
        return dict(self._positions)

    def has_position(self, symbol: str) -> bool:
        return self._positions.get(symbol, 0.0) > 0

    def largest_holding_symbol(self) -> str | None:
        if not self._positions:
            return None
        return max(self._positions.items(), key=lambda item: item[1])[0]


class StrategyContext:
    def __init__(
        self,
        *,
        trade_date: str,
        signal_date: str | None,
        history: HistoricalMarketView,
        reference: ReferenceDataView,
        portfolio: PortfolioView,
        market_data: SqliteMarketData | None = None,
    ):
        self.trade_date = trade_date
        self.signal_date = signal_date
        self.history = history
        self.reference = reference
        self.portfolio = portfolio
        self._market_data = market_data
        self._target_weights: Dict[str, float] | None = None
        self._execution_request: PriceRequest | None = None
        self._execution_prices: Dict[str, float] | None = None
        self._mark_request: PriceRequest | None = None
        self._mark_prices: Dict[str, float] | None = None
        self._write_offs: list[tuple[str, str]] = []

    def rebalance_to_weights(
        self,
        target_weights: Mapping[str, float],
        *,
        execution_request: PriceRequest | None = None,
        execution_prices: Mapping[str, float] | None = None,
    ) -> None:
        if execution_request is None and execution_prices is None:
            raise ValueError("execution_request or execution_prices is required")
        self._target_weights = {str(k): float(v) for k, v in target_weights.items()}
        self._execution_request = execution_request
        self._execution_prices = (
            {str(k): float(v) for k, v in execution_prices.items()}
            if execution_prices is not None
            else None
        )

    def set_mark_request(self, request: PriceRequest | None = None, *, prices: Mapping[str, float] | None = None) -> None:
        if request is None and prices is None:
            raise ValueError("request or prices is required")
        self._mark_request = request
        self._mark_prices = (
            {str(k): float(v) for k, v in prices.items()} if prices is not None else None
        )

    def current_price_map(
        self,
        *,
        request: PriceRequest,
        symbols: Sequence[str],
    ) -> Dict[str, float]:
        if self._market_data is None:
            raise RuntimeError("market_data is not available in this strategy context")
        return self._market_data.get_price_map(
            request=request,
            symbols=symbols,
            trade_date=self.trade_date,
        )

    def current_hk_to_cny_rate(self) -> float | None:
        if self._market_data is None:
            raise RuntimeError("market_data is not available in this strategy context")
        return self._market_data.get_hk_to_cny_rate(self.trade_date)

    def request_write_off(self, symbol: str, *, reason: str = "delist") -> None:
        self._write_offs.append((str(symbol), str(reason)))

    @property
    def target_weights(self) -> Mapping[str, float] | None:
        return self._target_weights

    @property
    def execution_request(self) -> PriceRequest | None:
        return self._execution_request

    @property
    def execution_prices(self) -> Mapping[str, float] | None:
        return self._execution_prices

    @property
    def mark_request(self) -> PriceRequest | None:
        return self._mark_request

    @property
    def mark_prices(self) -> Mapping[str, float] | None:
        return self._mark_prices

    @property
    def write_offs(self) -> Sequence[tuple[str, str]]:
        return tuple(self._write_offs)


__all__ = [
    "HistoricalMarketView",
    "PortfolioView",
    "PriceRequest",
    "ReferenceDataView",
    "SqliteMarketData",
    "StrategyContext",
]
