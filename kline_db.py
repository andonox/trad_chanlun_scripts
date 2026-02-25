# -*- coding: utf-8 -*-
"""
K 线数据 SQLite 存储：按 symbol + level 存 OHLCV 与 MACD，支持增量更新。
"""
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd


# 数据库文件放在项目 data 目录下
def _db_dir() -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(base, "data")
    os.makedirs(d, exist_ok=True)
    return d


def get_db_path() -> str:
    return os.path.join(_db_dir(), "stock_kline.db")


def get_connection() -> sqlite3.Connection:
    path = get_db_path()
    conn = sqlite3.connect(path)
    # WAL 模式：写入先到 .db-wal，主文件在 checkpoint 时才变大；读时会自动合并 .db + .db-wal，数据是完整的
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def checkpoint(mode: str = "FULL") -> tuple:
    """
    将 WAL 中的修改合并回主库文件，使 stock_kline.db 变大、-wal/-shm 缩小。
    mode: PASSIVE(不阻塞) / FULL(写回并截断 WAL) / RESTART / TRUNCATE
    返回 (busy, log_frames, checkpointed_frames)。若 checkpointed_frames=0 且 log_frames=0 表示 WAL 为空（本次可能无新写入）。
    """
    conn = get_connection()
    try:
        cur = conn.execute(f"PRAGMA wal_checkpoint({mode})")
        row = cur.fetchone()
        conn.commit()
        # SQLite 返回 (busy, log_frames, checkpointed_frames)
        return (row[0], row[1], row[2]) if row and len(row) >= 3 else (0, 0, 0)
    finally:
        conn.close()


def init_db(conn: Optional[sqlite3.Connection] = None) -> None:
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kline (
            symbol TEXT NOT NULL,
            level  TEXT NOT NULL,
            dt     TEXT NOT NULL,
            open   REAL, high REAL, low REAL, close REAL, volume REAL,
            dif    REAL, dea REAL, macd REAL,
            PRIMARY KEY (symbol, level, dt)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_symbol_level ON kline(symbol, level)")
    conn.commit()
    if close:
        conn.close()


def get_latest_dt(conn: sqlite3.Connection, symbol: str, level: str) -> Optional[str]:
    """返回该 symbol+level 在库中最新一条的 dt 字符串。"""
    row = conn.execute(
        "SELECT dt FROM kline WHERE symbol = ? AND level = ? ORDER BY dt DESC LIMIT 1",
        (symbol, level),
    ).fetchone()
    return row[0] if row else None


def read_klines(
    conn: sqlite3.Connection,
    symbol: str,
    level: str,
    min_dt: Optional[str] = None,
    max_dt: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    从库中读取 K 线。返回列含 date, open, high, low, close, volume, dif, dea, macd。
    date 由 dt 解析为 datetime，便于与原有逻辑兼容。
    """
    sql = "SELECT dt, open, high, low, close, volume, dif, dea, macd FROM kline WHERE symbol = ? AND level = ?"
    params: List = [symbol, level]
    if min_dt is not None:
        sql += " AND dt >= ?"
        params.append(min_dt)
    if max_dt is not None:
        sql += " AND dt <= ?"
        params.append(max_dt)
    sql += " ORDER BY dt ASC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "dif", "dea", "macd"])
    df = df.rename(columns={"dt": "date"})
    # 尝试混合格式解析
    df["date"] = pd.to_datetime(df["date"], format="mixed")
    return df


def save_klines(conn: sqlite3.Connection, df: pd.DataFrame, symbol: str, level: str) -> int:
    """
    将 DataFrame 写入库。df 需含 date/open/high/low/close/volume，以及可选的 dif, dea, macd。
    使用 INSERT OR REPLACE 覆盖同 (symbol, level, dt) 的旧数据。返回写入行数。
    """
    if df is None or df.empty:
        return 0
    df = df.copy()
    if "date" not in df.columns:
        return 0
    # 统一 dt 为字符串：日期型保留 YYYY-MM-DD，带时间的保留前 19 位
    df["dt"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S").str.replace(r" 00:00:00$", "", regex=True)
    for col in ("dif", "dea", "macd"):
        if col not in df.columns:
            df[col] = None
    df = df[["dt", "open", "high", "low", "close", "volume", "dif", "dea", "macd"]].dropna(subset=["open", "close"])
    conn.executemany(
        """INSERT OR REPLACE INTO kline (symbol, level, dt, open, high, low, close, volume, dif, dea, macd)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (symbol, level, r['dt'], r['open'], r['high'], r['low'], r['close'], r['volume'], r['dif'], r['dea'], r['macd'])
            for _, r in df.iterrows()
        ],
    )
    conn.commit()
    return len(df)


def delete_klines(conn: sqlite3.Connection, symbol: str, level: str, before_dt: Optional[str] = None) -> int:
    """删除指定 symbol+level 在 before_dt 之前的数据（可选）。用于清理或重拉。"""
    if before_dt is None:
        n = conn.execute("DELETE FROM kline WHERE symbol = ? AND level = ?", (symbol, level)).rowcount
    else:
        n = conn.execute("DELETE FROM kline WHERE symbol = ? AND level = ? AND dt < ?", (symbol, level, before_dt)).rowcount
    conn.commit()
    return n
