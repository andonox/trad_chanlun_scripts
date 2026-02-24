# -*- coding: utf-8 -*-
"""
A股多周期行情数据获取，含 MACD 计算。数据优先从本地 SQLite 读，缺或过期则增量拉取并写入库。
"""
import time
from datetime import datetime, timedelta
from typing import Optional

import akshare as ak
import pandas as pd
import numpy as np

from kline_db import get_connection, init_db, get_latest_dt, read_klines, save_klines

# 请求失败时重试次数与间隔（秒）
FETCH_RETRIES = 2
FETCH_RETRY_DELAY = 1.5

# 日线：库内最新日期 >= 今天 才视为已是最新（否则会拉取到今日）
# 分钟：库内最新一条的日期为今天则视为已是最新

# akshare 返回列名可能为中文，统一映射
OHLCV_CN = {"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"}
OHLCV_CN_MIN = {"时间": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"}


def _normalize_columns(df: pd.DataFrame, is_minute: bool = False) -> pd.DataFrame:
    """将 akshare 返回的中文列名统一为英文。"""
    mapping = OHLCV_CN_MIN if is_minute else OHLCV_CN
    rename = {k: v for k, v in mapping.items() if k in df.columns}
    if rename:
        df = df.rename(columns=rename)
    # 确保有 date 列且为 datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """计算 MACD。返回 DataFrame 含 dif, dea, macd 列。"""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd = (dif - dea) * 2
    return pd.DataFrame({"dif": dif, "dea": dea, "macd": macd})


def _fetch_daily_from_api(symbol: str, start_str: str, end_str: str, adjust: str = "qfq") -> Optional[pd.DataFrame]:
    """从 akshare 拉取日线并归一化、计算 MACD。"""
    for attempt in range(FETCH_RETRIES + 1):
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str, adjust=adjust)
            if df is not None and not df.empty:
                df = _normalize_columns(df, is_minute=False)
                macd_df = calc_macd(df["close"])
                df = pd.concat([df, macd_df], axis=1)
                return df
            return None
        except Exception:
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)
            else:
                return None
    return None


def get_daily_kline(symbol: str, months: int = 1, adjust: str = "qfq") -> Optional[pd.DataFrame]:
    """获取日线 K 线，至少 months 个月，前复权。优先读库，非最新则增量拉取并写入库。"""
    conn = get_connection()
    try:
        init_db(conn)
        today = datetime.now().strftime("%Y-%m-%d")
        min_dt = (datetime.now() - timedelta(days=months * 31 + 60)).strftime("%Y-%m-%d")
        latest_dt = get_latest_dt(conn, symbol, "daily")
        need_fetch = latest_dt is None or latest_dt < today
        if need_fetch:
            if latest_dt:
                start_ts = pd.Timestamp(latest_dt) + timedelta(days=1)
                start_str = start_ts.strftime("%Y%m%d")
            else:
                start_str = (datetime.now() - timedelta(days=months * 31 + 60)).strftime("%Y%m%d")
            end_str = datetime.now().strftime("%Y%m%d")
            df_new = _fetch_daily_from_api(symbol, start_str, end_str, adjust)
            if df_new is not None and not df_new.empty:
                save_klines(conn, df_new, symbol, "daily")
        result = read_klines(conn, symbol, "daily", min_dt=min_dt)
    finally:
        conn.close()
    if result is None or result.empty or len(result) < 50:
        return None
    return result


def _fetch_minute_from_api(symbol: str, period: str, start_str: str, end_str: str) -> Optional[pd.DataFrame]:
    """从 akshare 拉取分钟 K 线并归一化、计算 MACD。"""
    for attempt in range(FETCH_RETRIES + 1):
        try:
            df = ak.stock_zh_a_hist_min_em(
                symbol=symbol, period=period, start_date=start_str, end_date=end_str, adjust=""
            )
            if df is not None and not df.empty:
                df = _normalize_columns(df, is_minute=True)
                macd_df = calc_macd(df["close"])
                df = pd.concat([df, macd_df], axis=1)
                return df
            return None
        except Exception:
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)
            else:
                return None
    return None


def get_minute_kline(symbol: str, period: str, days: int = 5) -> Optional[pd.DataFrame]:
    """
    获取分钟 K 线。period: '1','5','15','30','60'。
    优先读库；若库内最新不是今天则拉取最近 days 天并写入库，再从库返回。
    """
    level = period  # 库中 level 与 period 一致
    conn = get_connection()
    try:
        init_db(conn)
        today = datetime.now().strftime("%Y-%m-%d")
        min_dt = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        latest_dt = get_latest_dt(conn, symbol, level)
        need_fetch = latest_dt is None or (latest_dt[:10] if len(latest_dt) >= 10 else latest_dt) < today
        if need_fetch:
            start_str = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d 09:30:00")
            end_str = datetime.now().strftime("%Y-%m-%d 15:00:00")
            df_new = _fetch_minute_from_api(symbol, period, start_str, end_str)
            if df_new is not None and not df_new.empty:
                save_klines(conn, df_new, symbol, level)
        result = read_klines(conn, symbol, level, min_dt=min_dt)
    finally:
        conn.close()
    if result is None or result.empty or len(result) < 30:
        return None
    return result


def get_kline_by_level(symbol: str, level: str, months: int = 1) -> Optional[pd.DataFrame]:
    """
    按级别获取 K 线。level: 'daily','60','30','15','5','1'。
    日线保证至少 months 个月；分钟线用最近约 5 个交易日（接口限制）。
    """
    if level == "daily":
        return get_daily_kline(symbol, months=months)
    if level in ("1", "5", "15", "30", "60"):
        return get_minute_kline(symbol, period=level, days=min(10, months * 22))
    return None


# 供主程序使用的级别列表（名称与 akshare 一致）
LEVELS = ["daily", "60", "30", "15", "5"]
