from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from qs.sqlite_utils import connect_sqlite

from ..models.dto import ComboResult, StandardSnapshot, StrategyDefinition, StrategyLatestRecord


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class WebDB:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def init_db(self) -> None:
        con = connect_sqlite(self.db_path)
        try:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS strategy_definition (
                    strategy_key TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    category TEXT NOT NULL,
                    module_path TEXT,
                    class_name TEXT,
                    feed_type TEXT NOT NULL,
                    default_params_json TEXT NOT NULL,
                    param_schema_json TEXT NOT NULL,
                    default_benchmarks_json TEXT NOT NULL,
                    supports_composer INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'active',
                    source_type TEXT NOT NULL DEFAULT 'framework'
                );

                CREATE TABLE IF NOT EXISTS strategy_run (
                    run_id TEXT PRIMARY KEY,
                    strategy_key TEXT NOT NULL,
                    run_tag TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    as_of_date TEXT NOT NULL,
                    initial_cash REAL NOT NULL,
                    status TEXT NOT NULL,
                    output_dir TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'framework',
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    error_message TEXT,
                    FOREIGN KEY(strategy_key) REFERENCES strategy_definition(strategy_key) ON DELETE CASCADE
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_run_tag ON strategy_run(run_tag);
                CREATE INDEX IF NOT EXISTS idx_strategy_run_latest ON strategy_run(strategy_key, completed_at DESC);

                CREATE TABLE IF NOT EXISTS strategy_metric (
                    run_id TEXT NOT NULL,
                    metric_key TEXT NOT NULL,
                    metric_value REAL,
                    PRIMARY KEY (run_id, metric_key),
                    FOREIGN KEY(run_id) REFERENCES strategy_run(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS strategy_equity_point (
                    run_id TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    nav REAL NOT NULL,
                    PRIMARY KEY (run_id, trade_date),
                    FOREIGN KEY(run_id) REFERENCES strategy_run(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS strategy_benchmark_point (
                    run_id TEXT NOT NULL,
                    benchmark_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    nav REAL NOT NULL,
                    PRIMARY KEY (run_id, benchmark_code, trade_date),
                    FOREIGN KEY(run_id) REFERENCES strategy_run(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS strategy_holding_snapshot (
                    run_id TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    price_cny REAL NOT NULL,
                    quantity REAL NOT NULL,
                    market_value REAL NOT NULL,
                    raw_weight REAL NOT NULL,
                    kelly_weight REAL NOT NULL,
                    source_strategy_weight REAL NOT NULL,
                    PRIMARY KEY (run_id, snapshot_date, symbol),
                    FOREIGN KEY(run_id) REFERENCES strategy_run(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS strategy_rebalance_event (
                    run_id TEXT NOT NULL,
                    rebalance_date TEXT NOT NULL,
                    signal_date TEXT,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, rebalance_date, payload_json),
                    FOREIGN KEY(run_id) REFERENCES strategy_run(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS combo_run (
                    combo_run_id TEXT PRIMARY KEY,
                    selected_strategies_json TEXT NOT NULL,
                    optimizer_config_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS combo_component_weight (
                    combo_run_id TEXT NOT NULL,
                    strategy_key TEXT NOT NULL,
                    raw_weight REAL NOT NULL,
                    kelly_weight REAL NOT NULL,
                    PRIMARY KEY (combo_run_id, strategy_key),
                    FOREIGN KEY(combo_run_id) REFERENCES combo_run(combo_run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS combo_equity_point (
                    combo_run_id TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    nav REAL NOT NULL,
                    PRIMARY KEY (combo_run_id, trade_date),
                    FOREIGN KEY(combo_run_id) REFERENCES combo_run(combo_run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS combo_holding_snapshot (
                    combo_run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    raw_weight REAL NOT NULL,
                    kelly_weight REAL NOT NULL,
                    PRIMARY KEY (combo_run_id, symbol),
                    FOREIGN KEY(combo_run_id) REFERENCES combo_run(combo_run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS job_run (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    message TEXT
                );

                CREATE TABLE IF NOT EXISTS app_setting (
                    setting_key TEXT PRIMARY KEY,
                    setting_value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_user (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    last_login_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            con.commit()
        finally:
            con.close()

    def upsert_strategy_definitions(self, definitions: list[StrategyDefinition]) -> None:
        con = connect_sqlite(self.db_path)
        try:
            for definition in definitions:
                rec = definition.to_record()
                con.execute(
                    """
                    INSERT INTO strategy_definition (
                        strategy_key, display_name, description, category, module_path,
                        class_name, feed_type, default_params_json, param_schema_json,
                        default_benchmarks_json, supports_composer, status, source_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(strategy_key) DO UPDATE SET
                        display_name=excluded.display_name,
                        description=excluded.description,
                        category=excluded.category,
                        module_path=excluded.module_path,
                        class_name=excluded.class_name,
                        feed_type=excluded.feed_type,
                        default_params_json=excluded.default_params_json,
                        param_schema_json=excluded.param_schema_json,
                        default_benchmarks_json=excluded.default_benchmarks_json,
                        supports_composer=excluded.supports_composer,
                        status=excluded.status,
                        source_type=excluded.source_type
                    """,
                    (
                        rec["strategy_key"],
                        rec["display_name"],
                        rec["description"],
                        rec["category"],
                        rec["module_path"],
                        rec["class_name"],
                        rec["feed_type"],
                        json.dumps(rec["default_params_json"], ensure_ascii=False, sort_keys=True),
                        json.dumps(rec["param_schema_json"], ensure_ascii=False, sort_keys=True),
                        json.dumps(
                            rec["default_benchmarks_json"], ensure_ascii=False, sort_keys=True
                        ),
                        int(rec["supports_composer"]),
                        rec["status"],
                        rec["source_type"],
                    ),
                )
            con.commit()
        finally:
            con.close()

    def save_snapshot(self, snapshot: StandardSnapshot) -> None:
        con = connect_sqlite(self.db_path)
        try:
            created_at = utc_now_iso()
            con.execute(
                """
                INSERT INTO strategy_run (
                    run_id, strategy_key, run_tag, params_json, start_date, end_date,
                    as_of_date, initial_cash, status, output_dir, source_type,
                    created_at, completed_at, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', ?, ?, ?, ?, NULL)
                """,
                (
                    snapshot.run_id,
                    snapshot.strategy_key,
                    snapshot.run_tag,
                    json.dumps(snapshot.params, ensure_ascii=False, sort_keys=True),
                    snapshot.start_date,
                    snapshot.end_date,
                    snapshot.as_of_date,
                    snapshot.initial_cash,
                    snapshot.output_dir,
                    snapshot.source_type,
                    created_at,
                    created_at,
                ),
            )
            for key, value in snapshot.metrics.items():
                metric_value = None
                if isinstance(value, (int, float)):
                    metric_value = float(value)
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_metric (run_id, metric_key, metric_value)
                    VALUES (?, ?, ?)
                    """,
                    (snapshot.run_id, key, metric_value),
                )
            con.executemany(
                """
                INSERT INTO strategy_equity_point (run_id, trade_date, nav)
                VALUES (?, ?, ?)
                """,
                [(snapshot.run_id, p.trade_date, p.nav) for p in snapshot.equity_curve],
            )
            con.executemany(
                """
                INSERT INTO strategy_benchmark_point (run_id, benchmark_code, trade_date, nav)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (snapshot.run_id, p.benchmark_code, p.trade_date, p.nav)
                    for p in snapshot.benchmarks
                ],
            )
            con.executemany(
                """
                INSERT INTO strategy_holding_snapshot (
                    run_id, snapshot_date, symbol, symbol_name, market, price_cny,
                    quantity, market_value, raw_weight, kelly_weight, source_strategy_weight
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot.run_id,
                        snapshot.as_of_date,
                        h.symbol,
                        h.symbol_name,
                        h.market,
                        h.price_cny,
                        h.quantity,
                        h.market_value,
                        h.raw_weight,
                        h.kelly_weight,
                        h.source_strategy_weight,
                    )
                    for h in snapshot.holdings
                ],
            )
            con.executemany(
                """
                INSERT INTO strategy_rebalance_event (run_id, rebalance_date, signal_date, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        snapshot.run_id,
                        str(item.get("rebalance_date") or item.get("trade_date") or snapshot.as_of_date),
                        item.get("signal_date"),
                        json.dumps(item, ensure_ascii=False, sort_keys=True),
                    )
                    for item in snapshot.rebalance_history
                ],
            )
            con.execute(
                """
                INSERT OR REPLACE INTO app_setting (setting_key, setting_value)
                VALUES ('last_snapshot_refresh_at', ?)
                """,
                (created_at,),
            )
            con.commit()
        finally:
            con.close()

    def save_combo_result(self, result: ComboResult) -> None:
        con = connect_sqlite(self.db_path)
        try:
            now = utc_now_iso()
            con.execute(
                """
                INSERT INTO combo_run (
                    combo_run_id, selected_strategies_json, optimizer_config_json,
                    metrics_json, status, created_at
                ) VALUES (?, ?, ?, ?, 'success', ?)
                """,
                (
                    result.combo_run_id,
                    json.dumps(result.selected_strategies, ensure_ascii=False),
                    json.dumps(result.optimizer_config, ensure_ascii=False, sort_keys=True),
                    json.dumps(result.metrics, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            con.executemany(
                """
                INSERT INTO combo_component_weight (combo_run_id, strategy_key, raw_weight, kelly_weight)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        result.combo_run_id,
                        item.strategy_key,
                        item.raw_weight,
                        item.kelly_weight,
                    )
                    for item in result.component_weights
                ],
            )
            con.executemany(
                """
                INSERT INTO combo_equity_point (combo_run_id, trade_date, nav)
                VALUES (?, ?, ?)
                """,
                [(result.combo_run_id, p.trade_date, p.nav) for p in result.equity_curve],
            )
            con.executemany(
                """
                INSERT INTO combo_holding_snapshot (
                    combo_run_id, symbol, symbol_name, market, raw_weight, kelly_weight
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        result.combo_run_id,
                        h.symbol,
                        h.symbol_name,
                        h.market,
                        h.raw_weight,
                        h.kelly_weight,
                    )
                    for h in result.holdings
                ],
            )
            con.commit()
        finally:
            con.close()

    def list_strategies(self) -> list[StrategyLatestRecord]:
        con = connect_sqlite(self.db_path, read_only=True)
        con.row_factory = sqlite3.Row
        try:
            rows = con.execute(
                """
                WITH latest AS (
                    SELECT sr.*
                    FROM strategy_run sr
                    JOIN (
                        SELECT strategy_key, MAX(completed_at) AS completed_at
                        FROM strategy_run
                        WHERE status='success'
                        GROUP BY strategy_key
                    ) x
                      ON sr.strategy_key=x.strategy_key AND sr.completed_at=x.completed_at
                )
                SELECT
                    d.*,
                    l.run_id AS latest_run_id,
                    l.run_tag AS latest_run_tag,
                    l.completed_at AS latest_completed_at,
                    MAX(CASE WHEN m.metric_key='cagr' THEN m.metric_value END) AS cagr,
                    MAX(CASE WHEN m.metric_key='ann_return' THEN m.metric_value END) AS ann_return,
                    MAX(CASE WHEN m.metric_key='ann_vol' THEN m.metric_value END) AS ann_vol,
                    MAX(CASE WHEN m.metric_key='sharpe' THEN m.metric_value END) AS sharpe,
                    MAX(CASE WHEN m.metric_key='max_drawdown' THEN m.metric_value END) AS max_drawdown,
                    COUNT(h.symbol) AS holding_count
                FROM strategy_definition d
                LEFT JOIN latest l ON l.strategy_key=d.strategy_key
                LEFT JOIN strategy_metric m ON m.run_id=l.run_id
                LEFT JOIN strategy_holding_snapshot h ON h.run_id=l.run_id
                GROUP BY
                    d.strategy_key, d.display_name, d.description, d.category, d.module_path,
                    d.class_name, d.feed_type, d.default_params_json, d.param_schema_json,
                    d.default_benchmarks_json, d.supports_composer, d.status, d.source_type,
                    l.run_id, l.run_tag, l.completed_at
                ORDER BY d.category, d.display_name
                """
            ).fetchall()
            return [self._row_to_strategy_latest(row) for row in rows]
        finally:
            con.close()

    def get_strategy_definition(self, strategy_key: str) -> dict[str, Any] | None:
        con = connect_sqlite(self.db_path, read_only=True)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                "SELECT * FROM strategy_definition WHERE strategy_key=?",
                (strategy_key,),
            ).fetchone()
            if row is None:
                return None
            return self._decode_definition_row(row)
        finally:
            con.close()

    def get_strategy_latest(self, strategy_key: str) -> StrategyLatestRecord | None:
        rows = [row for row in self.list_strategies() if row.strategy_key == strategy_key]
        return rows[0] if rows else None

    def get_latest_run_id(self, strategy_key: str) -> str | None:
        con = connect_sqlite(self.db_path, read_only=True)
        try:
            row = con.execute(
                """
                SELECT run_id FROM strategy_run
                WHERE strategy_key=? AND status='success'
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (strategy_key,),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            con.close()

    def get_run_summary(self, run_id: str) -> dict[str, Any] | None:
        con = connect_sqlite(self.db_path, read_only=True)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                "SELECT * FROM strategy_run WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            metrics = {
                k: v
                for k, v in con.execute(
                    "SELECT metric_key, metric_value FROM strategy_metric WHERE run_id=?",
                    (run_id,),
                ).fetchall()
            }
            return {
                **dict(row),
                "params": json.loads(row["params_json"]),
                "metrics": metrics,
            }
        finally:
            con.close()

    def get_run_equity(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT trade_date, nav
            FROM strategy_equity_point
            WHERE run_id=?
            ORDER BY trade_date
            """,
            (run_id,),
        )

    def get_run_benchmarks(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT benchmark_code, trade_date, nav
            FROM strategy_benchmark_point
            WHERE run_id=?
            ORDER BY benchmark_code, trade_date
            """,
            (run_id,),
        )

    def get_run_holdings(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT symbol, symbol_name, market, price_cny, quantity, market_value,
                   raw_weight, kelly_weight, source_strategy_weight
            FROM strategy_holding_snapshot
            WHERE run_id=?
            ORDER BY kelly_weight DESC, raw_weight DESC, symbol
            """,
            (run_id,),
        )

    def get_run_rebalances(self, run_id: str) -> list[dict[str, Any]]:
        rows = self._fetch_rows(
            """
            SELECT rebalance_date, signal_date, payload_json
            FROM strategy_rebalance_event
            WHERE run_id=?
            ORDER BY rebalance_date DESC
            """,
            (run_id,),
        )
        return [
            {
                "rebalance_date": row["rebalance_date"],
                "signal_date": row["signal_date"],
                **json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._fetch_rows(
            """
            SELECT run_id, strategy_key, run_tag, status, as_of_date, output_dir, created_at, completed_at
            FROM strategy_run
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def get_setting(self, key: str) -> str | None:
        con = connect_sqlite(self.db_path, read_only=True)
        try:
            row = con.execute(
                "SELECT setting_value FROM app_setting WHERE setting_key=?",
                (key,),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            con.close()

    def upsert_user(
        self,
        *,
        username: str,
        password_hash: str,
        role: str,
        display_name: str,
        is_active: bool,
    ) -> None:
        now = utc_now_iso()
        con = connect_sqlite(self.db_path)
        try:
            con.execute(
                """
                INSERT INTO app_user (
                    username, password_hash, role, display_name, is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    password_hash=excluded.password_hash,
                    role=excluded.role,
                    display_name=excluded.display_name,
                    is_active=excluded.is_active,
                    updated_at=excluded.updated_at
                """,
                (
                    username,
                    password_hash,
                    role,
                    display_name,
                    int(is_active),
                    now,
                    now,
                ),
            )
            con.commit()
        finally:
            con.close()

    def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        con = connect_sqlite(self.db_path, read_only=True)
        con.row_factory = sqlite3.Row
        try:
            row = con.execute(
                """
                SELECT username, password_hash, role, display_name, is_active, last_login_at,
                       created_at, updated_at
                FROM app_user
                WHERE username=?
                """,
                (username,),
            ).fetchone()
            if row is None:
                return None
            rec = dict(row)
            rec["is_active"] = bool(rec["is_active"])
            return rec
        finally:
            con.close()

    def touch_user_login(self, username: str) -> None:
        now = utc_now_iso()
        con = connect_sqlite(self.db_path)
        try:
            con.execute(
                """
                UPDATE app_user
                SET last_login_at=?, updated_at=?
                WHERE username=?
                """,
                (now, now, username),
            )
            con.commit()
        finally:
            con.close()

    def cleanup_old_runs(self, *, strategy_key: str, retention_days: int) -> list[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        cutoff_iso = cutoff.replace(microsecond=0).isoformat()
        con = connect_sqlite(self.db_path)
        try:
            rows = con.execute(
                """
                SELECT run_id, output_dir
                FROM strategy_run
                WHERE strategy_key=? AND completed_at < ?
                """,
                (strategy_key, cutoff_iso),
            ).fetchall()
            run_ids = [str(row[0]) for row in rows]
            con.executemany("DELETE FROM strategy_run WHERE run_id=?", [(run_id,) for run_id in run_ids])
            con.commit()
            return [str(row[1]) for row in rows]
        finally:
            con.close()

    def _fetch_rows(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        con = connect_sqlite(self.db_path, read_only=True)
        con.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in con.execute(sql, params).fetchall()]
        finally:
            con.close()

    def _row_to_strategy_latest(self, row: sqlite3.Row) -> StrategyLatestRecord:
        return StrategyLatestRecord(
            strategy_key=row["strategy_key"],
            display_name=row["display_name"],
            description=row["description"],
            category=row["category"],
            status=row["status"],
            supports_composer=bool(row["supports_composer"]),
            source_type=row["source_type"],
            default_params=json.loads(row["default_params_json"]),
            default_benchmarks=json.loads(row["default_benchmarks_json"]),
            latest_run_id=row["latest_run_id"],
            latest_run_tag=row["latest_run_tag"],
            latest_completed_at=row["latest_completed_at"],
            metrics={
                "cagr": float(row["cagr"]) if row["cagr"] is not None else 0.0,
                "ann_return": float(row["ann_return"]) if row["ann_return"] is not None else 0.0,
                "ann_vol": float(row["ann_vol"]) if row["ann_vol"] is not None else 0.0,
                "sharpe": float(row["sharpe"]) if row["sharpe"] is not None else 0.0,
                "max_drawdown": float(row["max_drawdown"]) if row["max_drawdown"] is not None else 0.0,
            },
            holding_count=int(row["holding_count"] or 0),
        )

    def _decode_definition_row(self, row: sqlite3.Row) -> dict[str, Any]:
        rec = dict(row)
        rec["default_params"] = json.loads(rec.pop("default_params_json"))
        rec["param_schema"] = json.loads(rec.pop("param_schema_json"))
        rec["default_benchmarks"] = json.loads(rec.pop("default_benchmarks_json"))
        rec["supports_composer"] = bool(rec["supports_composer"])
        return rec
