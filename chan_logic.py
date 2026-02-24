# -*- coding: utf-8 -*-
"""
缠论中枢与第三买点判断（简化实现）。
第三买点：上涨趋势中，价格向上脱离中枢后回抽，回抽低点不跌破中枢上沿 ZG。
"""
from typing import Tuple, Optional, List, Dict, Any
import pandas as pd
import numpy as np


def find_zhongshu_simple(
    df: pd.DataFrame,
    segment_bars: int = 8,
    lookback: int = 50,
) -> Optional[Tuple[float, float]]:
    """
    简化中枢识别：取 lookback 内最近三段（每段 segment_bars 根 K 线）的重叠区间。
    返回 (ZD, ZG) 即中枢下沿、上沿；若无有效中枢返回 None。
    """
    if df is None or len(df) < segment_bars * 3:
        return None
    df = df.tail(lookback).copy()
    n = len(df)
    if n < segment_bars * 3:
        return None
    # 三段：前、中、后
    s1 = df.iloc[-segment_bars * 3 : -segment_bars * 2]
    s2 = df.iloc[-segment_bars * 2 : -segment_bars]
    s3 = df.iloc[-segment_bars:]
    h1, l1 = s1["high"].max(), s1["low"].min()
    h2, l2 = s2["high"].max(), s2["low"].min()
    h3, l3 = s3["high"].max(), s3["low"].min()
    # 中枢区间：重叠部分 ZG = min(三高), ZD = max(三低)
    zg = min(h1, h2, h3)
    zd = max(l1, l2, l3)
    if zg <= zd:
        return None
    return (zd, zg)


def is_third_buy_simple(
    df: pd.DataFrame,
    zd: float,
    zg: float,
    break_bars: int = 3,
    pullback_bars: int = 10,
    min_close_above_zg: float = 0.0,
) -> bool:
    """
    判断最近是否形成第三买点（简化）：
    1. 在 break_bars 内曾有收盘价突破 ZG（收盘 > ZG）；
    2. 突破之后在 pullback_bars 内出现回抽，且回抽最低价 > ZG（不跌回中枢）；
    3. 当前收盘价在 ZG 之上（或 >= ZG + min_close_above_zg）。
    """
    if df is None or len(df) < break_bars + pullback_bars:
        return False
    df = df.tail(break_bars + pullback_bars + 5).copy()
    close = df["close"].values
    low = df["low"].values
    n = len(close)
    # 从右往左找：先有突破，再在右侧（更近）有回抽不破 ZG
    broken = False
    break_idx = -1
    for i in range(n - 1, -1, -1):
        if close[i] > zg:
            broken = True
            break_idx = i
            break
    if not broken or break_idx < 0:
        return False
    # 突破之后：tail 中索引 0 最早、n-1 最近，突破在 break_idx，故突破后 = break_idx+1 到 n-1
    after_lows = low[break_idx + 1 :]
    if len(after_lows) == 0:
        return False
    # 回抽不破 ZG：突破后的最低价 > ZG
    min_after = np.min(after_lows)
    if min_after <= zg:
        return False
    # 当前（最后一根）收盘要在 ZG 之上
    if close[-1] < zg + min_close_above_zg:
        return False
    return True


def check_third_buy_at_today(
    df: pd.DataFrame,
    segment_bars: int = 8,
    lookback: int = 50,
    break_bars: int = 5,
    pullback_bars: int = 12,
) -> bool:
    """
    综合判断：在给定 K 线数据上是否存在有效中枢，且当前（最后一根）是否满足第三买点条件。
    要求最后一根 K 线为“当天”或最近一根，即筛选的是“当天符合第三买点”的标的。
    """
    zs = find_zhongshu_simple(df, segment_bars=segment_bars, lookback=lookback)
    if zs is None:
        return False
    zd, zg = zs
    return is_third_buy_simple(
        df,
        zd=zd,
        zg=zg,
        break_bars=break_bars,
        pullback_bars=pullback_bars,
        min_close_above_zg=0.0,
    )


def _is_third_buy_with_reasons(
    df: pd.DataFrame,
    zd: float,
    zg: float,
    break_bars: int = 5,
    pullback_bars: int = 10,
) -> Dict[str, Any]:
    """内部：在已知中枢 ZD,ZG 下判断第三买点，并返回各条件是否满足及说明。"""
    out = {"曾突破ZG": False, "回抽不破ZG": False, "当前收盘在ZG上": False, "原因": []}
    if df is None or len(df) < break_bars + pullback_bars + 2:
        out["原因"].append("K线数量不足")
        return out
    df = df.tail(break_bars + pullback_bars + 5).copy()
    close = df["close"].values
    low = df["low"].values
    n = len(close)
    broken = False
    break_idx = -1
    for i in range(n - 1, -1, -1):
        if close[i] > zg:
            broken = True
            break_idx = i
            break
    out["曾突破ZG"] = broken
    if not broken:
        out["原因"].append("近期无收盘价突破中枢上沿ZG")
        return out
    after_lows = low[break_idx + 1 :]
    min_after = np.min(after_lows) if len(after_lows) > 0 else zg
    out["回抽不破ZG"] = min_after > zg
    if not out["回抽不破ZG"]:
        out["原因"].append(f"突破后回抽最低价{min_after:.4f}<=ZG{zg:.4f}")
    out["当前收盘在ZG上"] = close[-1] >= zg
    if not out["当前收盘在ZG上"]:
        out["原因"].append(f"当前收盘{close[-1]:.4f}<ZG{zg:.4f}")
    return out


def check_third_buy_detail(
    df: pd.DataFrame,
    segment_bars: int = 6,
    lookback: int = 50,
    break_bars: int = 5,
    pullback_bars: int = 10,
) -> Dict[str, Any]:
    """
    综合判断第三买点并返回详细条件结果，便于打 log。
    返回 dict：中枢存在, ZD, ZG, 曾突破ZG, 回抽不破ZG, 当前收盘在ZG上, 结论, 原因列表。
    """
    detail = {
        "中枢存在": False,
        "ZD": None,
        "ZG": None,
        "曾突破ZG": False,
        "回抽不破ZG": False,
        "当前收盘在ZG上": False,
        "结论": False,
        "原因": [],
    }
    if df is None or len(df) < 30:
        detail["原因"].append("K线不足30根")
        return detail
    zs = find_zhongshu_simple(df, segment_bars=segment_bars, lookback=lookback)
    if zs is None:
        detail["原因"].append("未识别到有效中枢(三段无重叠)")
        return detail
    zd, zg = zs
    detail["中枢存在"] = True
    detail["ZD"] = round(zd, 4)
    detail["ZG"] = round(zg, 4)
    sub = _is_third_buy_with_reasons(df, zd=zd, zg=zg, break_bars=break_bars, pullback_bars=pullback_bars)
    detail["曾突破ZG"] = sub["曾突破ZG"]
    detail["回抽不破ZG"] = sub["回抽不破ZG"]
    detail["当前收盘在ZG上"] = sub["当前收盘在ZG上"]
    detail["原因"] = sub.get("原因", [])
    detail["结论"] = detail["曾突破ZG"] and detail["回抽不破ZG"] and detail["当前收盘在ZG上"]
    if detail["结论"]:
        detail["原因"] = []  # 满足时原因清空或写“全部满足”
    return detail


def filter_levels_third_buy(
    kline_by_level: dict,
    levels: List[str],
) -> List[str]:
    """
    对多级别 K 线分别判断第三买点，返回满足条件的级别列表。
    kline_by_level: { "daily": df, "60": df, ... }
    """
    result = []
    for level in levels:
        df = kline_by_level.get(level)
        if df is None or len(df) < 30:
            continue
        lookback = 30 if level == "daily" else min(60, len(df) - 5)
        segment_bars = 6 if level == "daily" else 5
        if check_third_buy_at_today(
            df,
            segment_bars=segment_bars,
            lookback=lookback,
            break_bars=5,
            pullback_bars=10,
        ):
            result.append(level)
    return result


def filter_levels_third_buy_with_detail(
    kline_by_level: dict,
    levels: List[str],
) -> List[Tuple[str, Dict[str, Any], bool]]:
    """
    对多级别 K 线分别判断第三买点，返回 (级别, 详细条件dict, 是否符合) 列表，便于日志输出。
    """
    out = []
    for level in levels:
        df = kline_by_level.get(level)
        if df is None or len(df) < 30:
            out.append((level, {"原因": [f"无数据或K线不足(len={len(df) if df is not None else 0})"]}, False))
            continue
        lookback = 30 if level == "daily" else min(60, len(df) - 5)
        segment_bars = 6 if level == "daily" else 5
        detail = check_third_buy_detail(
            df,
            segment_bars=segment_bars,
            lookback=lookback,
            break_bars=5,
            pullback_bars=10,
        )
        out.append((level, detail, detail["结论"]))
    return out
