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


# ========== 底背驰判断 ==========

def find_recent_lows(df: pd.DataFrame, lookback: int = 20) -> List[Tuple[int, float, float]]:
    """
    找最近N天内的低点位置（用于底背驰判断）
    返回: [(索引, 低价, 对应DIF), ...]
    """
    df = df.tail(lookback).copy().reset_index(drop=True)
    lows = []

    for i in range(1, len(df) - 1):
        # 判断是否为局部低点：比前后都低
        if df.iloc[i]['low'] < df.iloc[i-1]['low'] and df.iloc[i]['low'] < df.iloc[i+1]['low']:
            lows.append((i, df.iloc[i]['low'], df.iloc[i]['dif']))

    # 按价格排序
    lows.sort(key=lambda x: x[1])
    return lows


def check_bottom_divergence(df: pd.DataFrame, lookback: int = 20) -> Dict[str, Any]:
    """
    底背驰判断（MACD底背驰）：
    1. 最近一次下跌创了新低（比较最近的两个低点）
    2. 对应的DIF没有创新低，反而比之前高

    返回: {
        "存在底背驰": bool,
        "价格新低": float,
        "MACD新低": float,
        "原因": []
    }
    """
    result = {
        "存在底背驰": False,
        "价格新低": None,
        "DIF新低": None,
        "MACD新低": None,
        "原因": []
    }

    if df is None or len(df) < 30:
        result["原因"].append("K线不足")
        return result

    # 需要MACD数据
    if "dif" not in df.columns or "dea" not in df.columns:
        result["原因"].append("无MACD数据")
        return result

    # 找最近的低点
    lows = find_recent_lows(df, lookback)

    if len(lows) < 2:
        result["原因"].append("找不到足够的低点")
        return result

    # 取最近的两个低点比较
    # lows已经按价格排序，最低价在最后
    # 最近的低价在列表末尾
    recent_low = lows[-1]  # 最近的低点
    prev_low = lows[-2]    # 之前一个低点

    current_idx, current_price, current_dif = recent_low
    prev_idx, prev_price, prev_dif = prev_low

    # 检查：最近低点创新低 AND DIF背离
    if current_price < prev_price:
        if current_dif > prev_dif:
            # DIF背驰
            result["存在底背驰"] = True
            result["价格新低"] = round(current_price, 2)
            result["DIF新低"] = round(current_dif, 4)
            result["前低位置DIF"] = round(prev_dif, 4)
        else:
            result["原因"].append("DIF也创新低，无背驰")
    else:
        result["原因"].append("价格未创新低")

    return result


def check_bottom_divergence_simple(df: pd.DataFrame) -> bool:
    """简化版底背驰判断"""
    return check_bottom_divergence(df)["存在底背驰"]


def calculate_divergence_strength(df: pd.DataFrame) -> Dict[str, Any]:
    """
    计算底背驰力度评分 (0-100分)

    评分因素:
    1. 价格跌幅得分 (0-30分): 创新低幅度越大，分数越高
    2. MACD背离得分 (0-40分): DIF背离幅度越大，分数越高
    3. MACD状态得分 (0-30分):
       - 当前MACD在零轴下方金叉: 30分
       - 当前MACD红柱且DIF>DEA: 20分
       - 当前MACD红柱: 15分
       - 其他: 5分
    """
    result = {
        "有底背驰": False,
        "力度评分": 0,
        "价格跌幅得分": 0,
        "背离得分": 0,
        "MACD状态得分": 0,
        "预估上涨概率": "",
        "预估上涨力度": "",
        "原因": []
    }

    if df is None or len(df) < 30:
        result["原因"].append("数据不足")
        return result

    if "dif" not in df.columns or "dea" not in df.columns:
        result["原因"].append("无MACD数据")
        return result

    df = df.tail(30).copy()

    # 使用与 check_bottom_divergence 相同的逻辑找局部低点
    lows = find_recent_lows(df, lookback=20)

    if len(lows) < 2:
        result["原因"].append("找不到足够的局部低点")
        return result

    # 取最近的两个低点比较
    recent_low = lows[-1]  # 最近的低点
    prev_low_item = lows[-2]    # 之前一个低点

    current_idx, current_price, current_dif = recent_low
    prev_idx, prev_price, prev_dif = prev_low_item

    # 检查是否有底背驰：最近低点创新低 AND DIF背离
    has_divergence = (current_price < prev_price and current_dif > prev_dif)

    if not has_divergence:
        result["原因"].append("无底背驰")
        return result

    result["有底背驰"] = True

    # ===== 1. 价格跌幅得分 =====
    price_drop_pct = (prev_price - current_price) / prev_price * 100
    # 跌幅越大得分越高，10%以上给满分30分
    price_score = min(30, int(price_drop_pct * 3))
    result["价格跌幅得分"] = price_score

    # ===== 2. MACD背离得分 =====
    # 计算背离幅度
    if current_dif >= prev_dif:
        div_pct = (current_dif - prev_dif) / abs(prev_dif) * 100 if prev_dif != 0 else 0
        div_score = min(40, int(div_pct * 2))
    else:
        # 如果DIF背离不成立，检查MACD柱状图
        macd_vals = df["macd"].values if "macd" in df.columns else (df["dif"].values - df["dea"].values)
        current_macd_val = macd_vals[current_idx]
        prev_macd_val = macd_vals[prev_idx]
        if current_macd_val >= prev_macd_val:
            div_pct = (current_macd_val - prev_macd_val) / abs(prev_macd_val) * 100 if prev_macd_val != 0 else 0
            div_score = min(40, int(div_pct * 2))
        else:
            div_score = 5
    result["背离得分"] = div_score

    # ===== 3. MACD状态得分 =====
    dif = df["dif"].values
    dea = df["dea"].values
    macd = df["macd"].values if "macd" in df.columns else (dif - dea)

    current_dea = dea[-1]
    current_dif_val = dif[-1]
    current_macd_val = macd[-1]

    if current_dea < 0 and current_dif_val > current_dea:
        # 零轴下方金叉
        macd_score = 30
    elif current_dif_val > current_dea and current_macd_val > 0:
        # 零轴上方多头
        macd_score = 20
    elif current_macd_val > 0:
        # 红柱
        macd_score = 15
    else:
        macd_score = 5
    result["MACD状态得分"] = macd_score

    # ===== 总分 =====
    result["力度评分"] = price_score + div_score + macd_score

    # ===== 预估上涨概率 =====
    score = result["力度评分"]
    if score >= 80:
        result["预估上涨概率"] = "极高 (>90%)"
    elif score >= 60:
        result["预估上涨概率"] = "高 (70-90%)"
    elif score >= 40:
        result["预估上涨概率"] = "中 (50-70%)"
    else:
        result["预估上涨概率"] = "低 (<50%)"

    # ===== 预估上涨力度 =====
    if score >= 70:
        result["预估上涨力度"] = "强劲"
    elif score >= 50:
        result["预估上涨力度"] = "中等"
    else:
        result["预估上涨力度"] = "较弱"

    return result


# ========== 缠论一买、二买、三买综合分析 ==========

def find_all_local_extrema(df: pd.DataFrame, lookback: int = 60) -> Dict[str, Any]:
    """
    找所有的局部高点和低点，用于识别一买和二买
    返回：{高点列表, 低点列表}
    """
    df = df.tail(lookback).copy().reset_index(drop=True)
    local_highs = []
    local_lows = []

    for i in range(1, len(df) - 1):
        # 局部高点
        if df.iloc[i]['high'] > df.iloc[i-1]['high'] and df.iloc[i]['high'] > df.iloc[i+1]['high']:
            local_highs.append({
                'idx': i,
                'price': df.iloc[i]['high'],
                'dif': df.iloc[i]['dif'],
                'dea': df.iloc[i]['dea'],
                'macd': df.iloc[i]['macd'],
                'date': df.iloc[i].get('date', str(i))
            })
        # 局部低点
        if df.iloc[i]['low'] < df.iloc[i-1]['low'] and df.iloc[i]['low'] < df.iloc[i+1]['low']:
            local_lows.append({
                'idx': i,
                'price': df.iloc[i]['low'],
                'dif': df.iloc[i]['dif'],
                'dea': df.iloc[i]['dea'],
                'macd': df.iloc[i]['macd'],
                'date': df.iloc[i].get('date', str(i))
            })

    return {'highs': local_highs, 'lows': local_lows}


def check_ma_cross(df: pd.DataFrame, ma1: int = 5, ma2: int = 10) -> Dict[str, Any]:
    """
    检查均线金叉/死叉
    返回: {
        "金叉": bool,
        "死叉": bool,
        "ma1": float,
        "ma2": float,
        "金叉日期": str,
    }
    """
    result = {
        "金叉": False,
        "死叉": False,
        "ma1": None,
        "ma2": None,
        "金叉日期": None,
    }

    if df is None or len(df) < ma2 + 5:
        return result

    # 计算均线
    df = df.copy()
    df['ma5'] = df['close'].rolling(window=ma1).mean()
    df['ma10'] = df['close'].rolling(window=ma2).mean()

    # 获取最近几天的均线值
    ma5 = df['ma5'].values
    ma10 = df['ma10'].values

    # 检查当前是否金叉 (MA5 > MA10)
    if ma5[-1] > ma10[-1]:
        result["金叉"] = True
        # 找到金叉日期
        for i in range(len(df) - 2, -1, -1):
            if ma5[i] <= ma10[i]:
                result["金叉日期"] = df.iloc[i].get('date', str(i))
                break

    # 检查当前是否死叉 (MA5 < MA10)
    if ma5[-1] < ma10[-1]:
        result["死叉"] = True

    result["ma1"] = ma5[-1]
    result["ma2"] = ma10[-1]

    return result


def check_volume_increase(df: pd.DataFrame, days: int = 5) -> Dict[str, Any]:
    """
    检查成交量是否放量
    返回: {
        "放量": bool,
        "量比": float,
        "今天量": float,
        "5日均量": float,
    }
    """
    result = {
        "放量": False,
        "量比": 0,
        "今天量": 0,
        "5日均量": 0,
    }

    if df is None or len(df) < days + 5:
        return result

    # 计算5日均量
    df = df.copy()
    df['vol_ma5'] = df['volume'].rolling(window=days).mean()

    today_vol = df['volume'].iloc[-1]
    vol_ma5 = df['vol_ma5'].iloc[-1]

    result["今天量"] = today_vol
    result["5日均量"] = vol_ma5

    if vol_ma5 > 0:
        result["量比"] = today_vol / vol_ma5
        result["放量"] = result["量比"] > 1.3  # 量比大于1.3视为放量

    return result


def check_ma多头排列(df: pd.DataFrame) -> Dict[str, Any]:
    """
    检查均线多头排列 (MA5 > MA10 > MA20 > MA60)
    返回: {
        "多头排列": bool,
        "ma5": float,
        "ma10": float,
        "ma20": float,
        "ma60": float,
    }
    """
    result = {
        "多头排列": False,
        "ma5": 0,
        "ma10": 0,
        "ma20": 0,
        "ma60": 0,
    }

    if df is None or len(df) < 65:
        return result

    df = df.copy()
    df['ma5'] = df['close'].rolling(window=5).mean()
    df['ma10'] = df['close'].rolling(window=10).mean()
    df['ma20'] = df['close'].rolling(window=20).mean()
    df['ma60'] = df['close'].rolling(window=60).mean()

    ma5 = df['ma5'].iloc[-1]
    ma10 = df['ma10'].iloc[-1]
    ma20 = df['ma20'].iloc[-1]
    ma60 = df['ma60'].iloc[-1]

    result["ma5"] = ma5
    result["ma10"] = ma10
    result["ma20"] = ma20
    result["ma60"] = ma60

    # 多头排列：5日 > 10日 > 20日 > 60日
    if ma5 > ma10 > ma20 > ma60:
        result["多头排列"] = True

    return result


def check_strong_breakout(df: pd.DataFrame) -> Dict[str, Any]:
    """
    检查强势突破（放量 + 涨幅大于5%）
    返回: {
        "强势突破": bool,
        "涨幅": float,
        "量比": float,
    }
    """
    result = {
        "强势突破": False,
        "涨幅": 0,
        "量比": 0,
    }

    if df is None or len(df) < 10:
        return result

    # 今天的涨幅
    today_change = (df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100

    # 量比
    vol_result = check_volume_increase(df)

    result["涨幅"] = today_change
    result["量比"] = vol_result.get("量比", 0)

    # 强势突破：涨幅>5% 且 量比>1.5
    if today_change > 5 and result["量比"] > 1.5:
        result["强势突破"] = True

    return result


def check_filters(df: pd.DataFrame) -> Dict[str, Any]:
    """
    综合筛选条件
    """
    result = {
        "满足条件": [],
        "不满足条件": [],
    }

    # 1. 放量检测
    vol = check_volume_increase(df)
    if vol["放量"]:
        result["满足条件"].append(f"放量(量比{vol['量比']:.2f})")
    else:
        result["不满足条件"].append(f"未放量(量比{vol['量比']:.2f})")

    # 2. 均线多头排列
    ma = check_ma多头排列(df)
    if ma["多头排列"]:
        result["满足条件"].append("均线多头排列")
    else:
        result["不满足条件"].append("均线非多头排列")

    # 3. 强势突破
    breakout = check_strong_breakout(df)
    if breakout["强势突破"]:
        result["满足条件"].append(f"强势突破(涨幅{breakout['涨幅']:.2f}%)")

    # 4. 底背驰+金叉
    div = check_bottom_divergence(df)
    ma_cross = check_ma_cross(df, 5, 10)
    if div["存在底背驰"] and ma_cross["金叉"]:
        result["满足条件"].append("底背驰+金叉")

    return result


def check_today_first_buy(df: pd.DataFrame, lookback: int = 30) -> Dict[str, Any]:
    """
    今天的买点：今天刚刚形成的一买
    - 今天形成底背驰（最近的低点在今天或昨天）
    - 或者今天MA5金叉MA10

    返回: {
        "今天一买": bool,
        "一买位置": float,
        "是否底背驰": bool,
        "均线金叉": bool,
        "原因": []
    }
    """
    result = {
        "今天一买": False,
        "一买位置": None,
        "是否底背驰": False,
        "均线金叉": False,
        "原因": []
    }

    if df is None or len(df) < 20:
        result["原因"].append("K线不足")
        return result

    # 检查今天的均线金叉
    df_ma = df.copy()
    df_ma['ma5'] = df_ma['close'].rolling(window=5).mean()
    df_ma['ma10'] = df_ma['close'].rolling(window=10).mean()

    # 昨天MA5 <= MA10, 今天MA5 > MA10 = 今天金叉
    if len(df_ma) >= 2:
        ma5_yesterday = df_ma['ma5'].iloc[-2]
        ma10_yesterday = df_ma['ma10'].iloc[-2]
        ma5_today = df_ma['ma5'].iloc[-1]
        ma10_today = df_ma['ma10'].iloc[-1]

        if ma5_yesterday <= ma10_yesterday and ma5_today > ma10_today:
            result["均线金叉"] = True
            result["今天一买"] = True
            result["一买位置"] = df['close'].iloc[-1]
            result["原因"].append(f"今天MA5金叉MA10，当前价{df['close'].iloc[-1]:.2f}")
            return result

    # 检查今天/昨天是否形成底背驰
    # 底背驰：最近的低点在最近2天内，且DIF背离
    if len(df) >= 10:
        recent_df = df.tail(10)
        lows = []
        for i in range(1, len(recent_df) - 1):
            if recent_df.iloc[i]['low'] < recent_df.iloc[i-1]['low'] and recent_df.iloc[i]['low'] < recent_df.iloc[i+1]['low']:
                lows.append((i, recent_df.iloc[i]['low'], recent_df.iloc[i]['dif']))

        if len(lows) >= 2:
            # 找最近的两个低点
            lows.sort(key=lambda x: x[1])
            recent_low = lows[0]  # 最低点
            prev_low = lows[1]    # 次低点

            # 最低点在最近3天内
            low_idx = recent_low[0]
            if low_idx <= 2:  # 最近3天
                current_price = recent_low[1]
                current_dif = recent_low[2]
                prev_price = prev_low[1]
                prev_dif = prev_low[2]

                if current_price < prev_price and current_dif > prev_dif:
                    result["今天一买"] = True
                    result["一买位置"] = current_price
                    result["是否底背驰"] = True
                    result["原因"].append(f"今天形成底背驰，价格新低{current_price:.2f}，DIF背离")
                    return result

    result["原因"].append("今天未形成一买")
    return result


def check_second_buy_point(df: pd.DataFrame, lookback: int = 80) -> Dict[str, Any]:
    """
    缠论二买判断（二买 / 第二买点）：
    - 在一买之后，价格上涨并形成中枢
    - 价格回抽，回抽低点不跌破一买位置
    - 当前价格在一买位置之上

    返回: {
        "存在二买": bool,
        "一买位置": float,
        "二买位置": float,
        "中枢ZG": float,
        "中枢ZD": float,
        "原因": []
    }
    """
    result = {
        "存在二买": False,
        "一买位置": None,
        "二买位置": None,
        "中枢ZG": None,
        "中枢ZD": None,
        "原因": []
    }

    if df is None or len(df) < 50:
        result["原因"].append("K线不足")
        return result

    # 先找到一买位置
    first_buy = check_first_buy_point(df, lookback)
    if not first_buy["存在一买"]:
        result["原因"].append("未找到一买")
        return result

    one_buy_price = first_buy["一买位置"]
    result["一买位置"] = one_buy_price

    # 找到一买之后的局部低点
    extrema = find_all_local_extrema(df, lookback)
    lows = extrema['lows']

    # 过滤掉一买位置之前的低点
    candidate_lows = [l for l in lows if l['price'] > one_buy_price]

    if len(candidate_lows) == 0:
        result["原因"].append("一买之后无明显回调")
        return result

    # 简化判断：直接看是否有回抽不破一买
    recent_low = min([l['price'] for l in candidate_lows]) if candidate_lows else None
    if recent_low and recent_low > one_buy_price:
        # 还需要当前价格在一买之上
        current_close = df['close'].iloc[-1]
        if current_close > one_buy_price:
            # 尝试找中枢
            one_buy_idx = -1
            for i, l in enumerate(lows):
                if abs(l['price'] - one_buy_price) < 0.01:
                    one_buy_idx = i
                    break

            if one_buy_idx >= 0 and one_buy_idx < len(lows) - 2:
                df_after = df.tail(lookback).copy()
                if len(df_after) > 30:
                    zs = find_zhongshu_simple(df_after, segment_bars=6, lookback=min(30, len(df_after) - 5))
                    if zs:
                        result["中枢ZD"], result["中枢ZG"] = zs

            result["存在二买"] = True
            result["二买位置"] = recent_low
            result["原因"].append(f"回抽低点{recent_low:.2f}高于一买{one_buy_price:.2f}，当前价{current_close:.2f}也在一买之上")

    if not result["存在二买"]:
        result["原因"].append("未形成有效的二买形态")

    return result


def check_third_buy_from_first_buy(df: pd.DataFrame, lookback: int = 80) -> Dict[str, Any]:
    """
    从一买开始找第三买点：
    一买之后上涨 -> 形成中枢 -> 突破中枢 -> 回抽不破ZG -> 形成三买
    """
    result = {
        "存在三买": False,
        "一买位置": None,
        "中枢ZG": None,
        "中枢ZD": None,
        "原因": []
    }

    if df is None or len(df) < 50:
        result["原因"].append("K线不足")
        return result

    # 找到一买
    first_buy = check_first_buy_point(df, lookback)
    if not first_buy["存在一买"]:
        result["原因"].append("未找到一买")
        return result

    result["一买位置"] = first_buy["一买位置"]

    # 找一买之后的中枢
    one_buy_idx = -1
    for i in range(len(df) - 1, -1, -1):
        if abs(df.iloc[i]['low'] - first_buy["一买位置"]) < 0.01:
            one_buy_idx = i
            break

    if one_buy_idx < 0:
        result["原因"].append("无法确定一买位置")
        return result

    # 取一买之后的K线
    df_after = df.iloc[one_buy_idx:].copy()
    if len(df_after) < 30:
        result["原因"].append("一买之后数据不足")
        return result

    # 识别中枢
    zs = find_zhongshu_simple(df_after, segment_bars=6, lookback=min(30, len(df_after) - 5))
    if zs is None:
        result["原因"].append("一买之后未形成有效中枢")
        return result

    zd, zg = zs
    result["中枢ZD"] = zd
    result["中枢ZG"] = zg

    # 检查三买条件
    if is_third_buy_simple(df_after, zd, zg):
        result["存在三买"] = True
        result["原因"].append("形成完整的一买->中枢->三买形态")
    else:
        result["原因"].append("未形成三买（可能中枢未突破或回抽跌破）")

    return result


def check_today_second_buy(df: pd.DataFrame, lookback: int = 30) -> Dict[str, Any]:
    """
    今天的二买：今天价格回抽到一买位置附近（回抽不破一买）
    或者：今天形成MA5/MA10金叉，且之前已经形成过一买
    """
    result = {
        "今天二买": False,
        "一买位置": None,
        "二买位置": None,
        "原因": []
    }

    if df is None or len(df) < 20:
        result["原因"].append("K线不足")
        return result

    # 找最近的一买位置（在历史数据中找）
    # 先找局部低点
    lows = []
    for i in range(1, len(df) - 1):
        if df.iloc[i]['low'] < df.iloc[i-1]['low'] and df.iloc[i]['low'] < df.iloc[i+1]['low']:
            lows.append((i, df.iloc[i]['low'], df.iloc[i].get('date', '')))

    if len(lows) < 2:
        result["原因"].append("找不到足够低点")
        return result

    # 找最低的局部低点作为一买
    lows.sort(key=lambda x: x[1])
    one_buy_price = lows[0][1]  # 最低价

    # 今天价格回抽到一买位置附近（±5%）
    current_price = df['close'].iloc[-1]
    pullback_range = one_buy_price * 0.05  # 5%范围

    if abs(current_price - one_buy_price) <= pullback_range:
        # 回抽到一买位置
        if current_price > one_buy_price:
            # 今天价格高于一买位置，形成二买
            result["今天二买"] = True
            result["一买位置"] = one_buy_price
            result["二买位置"] = current_price
            result["原因"].append(f"今天回抽到一买位置{one_buy_price:.2f}附近，当前价{current_price:.2f}")
            return result

    # 检查今天的MA金叉
    df_ma = df.copy()
    df_ma['ma5'] = df_ma['close'].rolling(window=5).mean()
    df_ma['ma10'] = df_ma['close'].rolling(window=10).mean()

    if len(df_ma) >= 2:
        ma5_yesterday = df_ma['ma5'].iloc[-2]
        ma10_yesterday = df_ma['ma10'].iloc[-2]
        ma5_today = df_ma['ma5'].iloc[-1]
        ma10_today = df_ma['ma10'].iloc[-1]

        if ma5_yesterday <= ma10_yesterday and ma5_today > ma10_today:
            # 今天金叉，且当前价格高于一买
            if current_price > one_buy_price:
                result["今天二买"] = True
                result["一买位置"] = one_buy_price
                result["二买位置"] = current_price
                result["原因"].append(f"今天MA金叉且价格高于一买，一买{one_buy_price:.2f}，当前{current_price:.2f}")
                return result

    result["原因"].append("今天未形成二买")
    return result


def check_today_third_buy(df: pd.DataFrame, lookback: int = 30) -> Dict[str, Any]:
    """
    今天的第三买点：
    1. 今天价格突破中枢ZG
    2. 今天价格回抽，回抽低点不跌破ZG
    3. 当前收盘价在ZG之上
    """
    result = {
        "今天三买": False,
        "中枢ZG": None,
        "中枢ZD": None,
        "当前价格": None,
        "原因": []
    }

    if df is None or len(df) < 30:
        result["原因"].append("K线不足")
        return result

    # 识别中枢
    zs = find_zhongshu_simple(df, segment_bars=6, lookback=min(lookback, len(df) - 10))
    if zs is None:
        result["原因"].append("未找到有效中枢")
        return result

    zd, zg = zs
    result["中枢ZD"] = zd
    result["中枢ZG"] = zg

    current_price = df['close'].iloc[-1]
    current_low = df['low'].iloc[-1]
    result["当前价格"] = current_price

    # 今天的条件：
    # 1. 今天收盘价 > ZG
    # 2. 今天的最低价 > ZG (回抽不破ZG)
    if current_price > zg and current_low > zg:
        # 还需要确认之前有突破ZG的动作
        # 检查最近5天内是否有突破
        recent_close = df['close'].tail(5).values
        if any(c > zg for c in recent_close):
            result["今天三买"] = True
            result["原因"].append(f"今天收盘价{current_price:.2f}>ZG{zg:.2f}，最低价{current_low:.2f}>ZG，未跌破")
        else:
            result["原因"].append("最近5天无突破ZG")
    else:
        if current_price <= zg:
            result["原因"].append(f"当前收盘{current_price:.2f}未突破ZG{zg:.2f}")
        if current_low <= zg:
            result["原因"].append(f"当前最低{current_low:.2f}跌破了ZG{zg:.2f}")

    return result


def analyze_today_buy_points(df: pd.DataFrame, lookback: int = 30) -> Dict[str, Any]:
    """
    分析今天的买点（一买、二买、三买）
    返回今天的实时信号
    """
    result = {
        "三买": check_today_third_buy(df, lookback),
        "二买": check_today_second_buy(df, lookback),
        "一买": check_today_first_buy(df, lookback),
        "今天信号": "无"
    }

    # 优先级：三买 > 二买 > 一买
    if result["三买"]["今天三买"]:
        result["今天信号"] = "三买"
    elif result["二买"]["今天二买"]:
        result["今天信号"] = "二买"
    elif result["一买"]["今天一买"]:
        result["今天信号"] = "一买"

    return result


def analyze_all_buy_points(df: pd.DataFrame, lookback: int = 80) -> Dict[str, Any]:
    """
    综合分析一买、二买、三买

    返回: {
        "一买": {...},
        "二买": {...},
        "三买": {...},
        "当前信号": str  # 优先级: 三买 > 二买 > 一买
    }
    """
    result = {
        "一买": check_first_buy_point(df, lookback),
        "二买": check_second_buy_point(df, lookback),
        "三买": check_third_buy_from_first_buy(df, lookback),
        "当前信号": "无"
    }

    # 确定当前信号
    if result["三买"]["存在三买"]:
        result["当前信号"] = "三买"
    elif result["二买"]["存在二买"]:
        result["当前信号"] = "二买"
    elif result["一买"]["存在一买"]:
        result["当前信号"] = "一买"

    return result
