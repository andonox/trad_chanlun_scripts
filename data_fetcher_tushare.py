# -*- coding: utf-8 -*-
"""
A股多周期行情数据获取 - Tushare数据源
支持批量获取所有股票数据，存储到本地后进行分析
"""
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pandas as pd
import numpy as np

from kline_db import get_connection, init_db, get_latest_dt, read_klines, save_klines
from config import TUSHARE_TOKEN

# 请求配置
FETCH_RETRIES = 2
FETCH_RETRY_DELAY = 1.0
BATCH_SIZE = 100  # 批量获取每次最多100只

# Tushare接口
try:
    import tushare as ts
    ts.set_token(TUSHARE_TOKEN)
    PRO = ts.pro_api()
except Exception as e:
    print(f"Tushare初始化失败: {e}")
    PRO = None


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """计算 MACD"""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd = (dif - dea) * 2
    return pd.DataFrame({"dif": dif, "dea": dea, "macd": macd})


def _to_tushare_tscode(code: str) -> str:
    """转换为Tushare的tscode格式"""
    code = str(code).zfill(6)
    if code.startswith('6'):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


def fetch_stock_daily(code: str, num: int = 300) -> Optional[pd.DataFrame]:
    """
    获取单只股票日线数据
    num: 获取数量
    """
    if PRO is None:
        return None

    tscode = _to_tushare_tscode(code)

    # 计算日期范围
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=num + 30)).strftime("%Y%m%d")

    for attempt in range(FETCH_RETRIES + 1):
        try:
            df = PRO.daily(ts_code=tscode, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return None

            # 只保留请求的股票
            df = df[df["ts_code"] == tscode]
            if df.empty:
                return None

            # 转换列名
            df = df.rename(columns={
                "trade_date": "date",
                "vol": "volume"
            })

            # 转换日期格式
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

            # 按日期排序（从早到晚）
            df = df.sort_values("date").reset_index(drop=True)

            # 转换数据类型
            for col in ["open", "close", "high", "low", "volume"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # 取最近的num条
            df = df.tail(num).reset_index(drop=True)

            # 计算MACD
            if len(df) > 0:
                macd_df = calc_macd(df["close"])
                df = pd.concat([df, macd_df], axis=1)

            return df

        except Exception as e:
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)
            else:
                return None

    return None


def fetch_batch_daily(codes: List[str], num: int = 300) -> Dict[str, pd.DataFrame]:
    """
    批量获取多只股票日线数据
    返回: {code: dataframe}
    """
    results = {}

    if PRO is None:
        print("Tushare未初始化")
        return results

    # 转换为tushare格式
    tscodes = [_to_tushare_tscode(code) for code in codes]

    # 计算日期范围
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=num + 30)).strftime("%Y%m%d")

    # 分批请求（tushare每次最多5000条）
    # 实际上每次请求可以包含多只股票
    batch_size = 100  # 每次最多100只

    for i in range(0, len(tscodes), batch_size):
        batch_tscodes = tscodes[i:i + batch_size]

        try:
            # 批量获取
            df = PRO.daily(ts_code=",".join(batch_tscodes), start_date=start_date, end_date=end_date)

            if df is None or df.empty:
                continue

            # 按股票分组
            for tscode in batch_tscodes:
                stock_df = df[df["ts_code"] == tscode].copy()
                if stock_df.empty:
                    continue

                # 提取原始代码
                code = tscode.split(".")[0]

                # 转换列名
                stock_df = stock_df.rename(columns={
                    "trade_date": "date",
                    "vol": "volume"
                })

                # 转换日期
                stock_df["date"] = pd.to_datetime(stock_df["date"], format="%Y%m%d")

                # 排序
                stock_df = stock_df.sort_values("date").reset_index(drop=True)

                # 转换数据类型
                for col in ["open", "close", "high", "low", "volume"]:
                    stock_df[col] = pd.to_numeric(stock_df[col], errors='coerce')

                # 取最近num条
                stock_df = stock_df.tail(num).reset_index(drop=True)

                # 计算MACD
                if len(stock_df) > 0:
                    macd_df = calc_macd(stock_df["close"])
                    stock_df = pd.concat([stock_df, macd_df], axis=1)

                results[code] = stock_df

        except Exception as e:
            print(f"批量获取第{i//batch_size + 1}批失败: {e}")
            time.sleep(FETCH_RETRY_DELAY)

    return results


def fetch_and_save_stock(code: str, levels: List[str] = None) -> Dict:
    """获取并保存单只股票数据，返回结果"""
    if levels is None:
        levels = ["daily"]

    result = {"code": code, "success": True, "levels": {}}

    conn = get_connection()
    try:
        init_db(conn)

        for level in levels:
            try:
                if level == "daily":
                    df = fetch_stock_daily(code, num=300)
                else:
                    # 分钟线
                    df = fetch_stock_minute(code, level, num=300)

                if df is not None and not df.empty:
                    save_klines(conn, df, code, level)
                    result["levels"][level] = len(df)
                else:
                    result["levels"][level] = 0

            except Exception as e:
                result["levels"][level] = -1

    finally:
        conn.close()

    return result


def fetch_stock_minute(code: str, period: str = "60", num: int = 300) -> Optional[pd.DataFrame]:
    """获取分钟线数据 - 使用stk_mins接口"""
    if PRO is None:
        return None

    tscode = _to_tushare_tscode(code)

    # 分钟线接口
    # period: 1/5/15/30/60 分钟
    freq_map = {"1": "1min", "5": "5min", "15": "15min", "30": "30min", "60": "60min"}
    freq = freq_map.get(period, "60min")

    # 获取最近num条，需要更大的日期范围
    end_time = datetime.now()
    start_time = end_time - timedelta(days=60)

    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

    for attempt in range(FETCH_RETRIES + 1):
        try:
            # 使用stk_mins接口获取历史分钟线
            df = PRO.stk_mins(
                ts_code=tscode,
                start_date=start_str,
                end_date=end_str,
                freq=freq
            )

            if df is None or df.empty:
                return None

            # 转换列名
            df = df.rename(columns={
                "trade_time": "date",
                "vol": "volume"
            })

            # 转换日期
            df["date"] = pd.to_datetime(df["date"])

            # 排序
            df = df.sort_values("date").reset_index(drop=True)

            # 转换类型
            for col in ["open", "close", "high", "low", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 取最近
            df = df.tail(num).reset_index(drop=True)

            # 计算MACD
            if len(df) > 0:
                macd_df = calc_macd(df["close"])
                df = pd.concat([df, macd_df], axis=1)

            return df

        except Exception as e:
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)
            else:
                return None

    return None


def batch_fetch_stocks(codes: List[str], levels: List[str] = None,
                       max_workers: int = 10,
                       delay: float = 0.1) -> List[Dict]:
    """
    批量获取多只股票数据
    使用Tushare批量接口
    """
    results = []
    total = len(codes)

    print(f"开始批量获取 {total} 只股票数据...")
    start_time = time.time()

    # 默认只获取日线
    if levels is None:
        levels = ["daily"]

    # 只处理日线
    if "daily" in levels:
        # 分批获取，每批100只
        for i in range(0, total, BATCH_SIZE):
            batch_codes = codes[i:i + BATCH_SIZE]
            batch_num = len(batch_codes)

            print(f"获取第 {i//BATCH_SIZE + 1} 批 ({batch_num} 只)...")

            # 批量获取
            batch_data = fetch_batch_daily(batch_codes, num=300)

            # 保存到数据库
            conn = get_connection()
            try:
                init_db(conn)
                saved_count = 0

                for code, df in batch_data.items():
                    if df is not None and not df.empty:
                        save_klines(conn, df, code, "daily")
                        saved_count += 1
                        results.append({"code": code, "success": True, "levels": {"daily": len(df)}})
                    else:
                        results.append({"code": code, "success": False, "levels": {"daily": 0}})

                conn.commit()
                print(f"  成功: {saved_count}/{batch_num}")

            except Exception as e:
                print(f"  保存失败: {e}")
            finally:
                conn.close()

            # 控制请求频率
            if delay > 0 and i + BATCH_SIZE < total:
                time.sleep(delay)

            # 打印进度
            elapsed = time.time() - start_time
            completed = min(i + BATCH_SIZE, total)
            print(f"进度: {completed}/{total} ({completed/total*100:.1f}%), 耗时: {elapsed:.1f}s")

    elapsed = time.time() - start_time
    success_count = sum(1 for r in results if r.get("success"))
    print(f"数据获取完成! 总耗时: {elapsed:.1f}s, 成功: {success_count}/{total}")

    return results


# 供主程序使用的级别列表
LEVELS = ["daily", "60", "30", "15", "5"]


def get_kline_by_level(code: str, level: str, months: int = 1) -> Optional[pd.DataFrame]:
    """从本地数据库读取K线"""
    conn = get_connection()
    try:
        init_db(conn)

        min_days = months * 31 + 60
        min_dt = (datetime.now() - timedelta(days=min_days)).strftime("%Y-%m-%d")

        result = read_klines(conn, code, level, min_dt=min_dt)
    finally:
        conn.close()

    if level == "daily":
        if result is None or result.empty or len(result) < 50:
            return None

    return result
