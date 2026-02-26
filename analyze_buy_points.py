# -*- coding: utf-8 -*-
"""
缠论一买、二买、三买综合分析脚本
分析A股所有股票的买卖点信号
"""
import sys
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import argparse
import time
from datetime import datetime
from typing import List, Dict
import pandas as pd

from data_fetcher_tushare import get_kline_by_level, batch_fetch_stocks
from chan_logic import (
    analyze_today_buy_points,
    check_today_first_buy,
    check_today_second_buy,
    check_today_third_buy,
    check_bottom_divergence,
    calculate_divergence_strength,
)
from kline_db import checkpoint as db_checkpoint


def generate_stock_list(limit: int = None) -> List[str]:
    """生成A股股票代码列表"""
    codes = []
    # 沪市主板
    codes.extend([f"{i:06d}" for i in range(600000, 604000)])
    # 深市主板
    codes.extend([f"{i:06d}" for i in range(1, 1000)])
    codes.extend([f"{i:06d}" for i in range(1000, 2000)])
    # 创业板
    codes.extend([f"{i:06d}" for i in range(300001, 301000)])

    if limit:
        codes = codes[:limit]
    return codes


def get_stock_names(codes: List[str]) -> Dict[str, str]:
    """获取股票名称"""
    try:
        import tushare as ts
        from config import TUSHARE_TOKEN
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()

        df = pro.stock_basic(list_status='L', fields='ts_code,name')
        if df is None or df.empty:
            return {}

        return dict(zip([str(x['ts_code'])[:6] for x in df.to_dict('records')],
                      [x['name'] for x in df.to_dict('records')]))
    except Exception as e:
        print(f"获取股票名称失败: {e}")
        return {}


def analyze_stock(code: str, levels: List[str] = None) -> List[Dict]:
    """分析单只股票的买卖点"""
    if levels is None:
        levels = ["daily"]

    results = []
    for level in levels:
        df = get_kline_by_level(code, level, months=2)
        if df is None or len(df) < 30:
            continue

        # 分析今天的买点
        analysis = analyze_today_buy_points(df)

        # 获取当前价格
        current_price = df['close'].iloc[-1]

        # 根据信号类型生成结果
        signal = analysis.get("今天信号", "无")

        if signal == "三买":
            third_buy = analysis.get("三买", {})
            results.append({
                "股票代码": code,
                "级别": level,
                "信号类型": "第三买点",
                "当前价格": round(current_price, 2),
                "中枢ZG": third_buy.get("中枢ZG"),
                "中枢ZD": third_buy.get("中枢ZD"),
                "原因": "; ".join(third_buy.get("原因", [])),
            })
        elif signal == "二买":
            second_buy = analysis.get("二买", {})
            one_buy = second_buy.get("一买位置")
            two_buy = second_buy.get("二买位置")

            # 计算评分指标
            # 1. 一买到二买的涨幅
            rise_pct = 0
            if one_buy and two_buy and one_buy > 0:
                rise_pct = (two_buy - one_buy) / one_buy * 100

            # 2. 二买到当前价格的涨幅（潜在上涨空间）
            potential_pct = 0
            if two_buy and current_price > 0:
                potential_pct = (current_price - two_buy) / two_buy * 100

            # 3. 中枢强度（中枢区间宽度/当前价格）
            zg = second_buy.get("中枢ZG")
            zd = second_buy.get("中枢ZD")
            z_strength = 0
            if zg and zd and zd > 0:
                z_strength = (zg - zd) / zd * 100

            # 综合评分 = 上涨空间 + 中枢强度 + 趋势强度
            score = potential_pct * 0.4 + rise_pct * 0.3 + z_strength * 0.3

            results.append({
                "股票代码": code,
                "级别": level,
                "信号类型": "第二买点",
                "当前价格": round(current_price, 2),
                "一买位置": round(one_buy, 2) if one_buy else None,
                "二买位置": round(two_buy, 2) if two_buy else None,
                "中枢ZG": round(zg, 2) if zg else None,
                "中枢ZD": round(zd, 2) if zd else None,
                "一买到二买涨幅": round(rise_pct, 2),
                "二买潜在涨幅": round(potential_pct, 2),
                "中枢强度": round(z_strength, 2),
                "综合评分": round(score, 2),
                "原因": "; ".join(second_buy.get("原因", [])),
            })
        elif signal == "一买":
            first_buy = analysis.get("一买", {})
            # 计算底背驰力度
            div_strength = calculate_divergence_strength(df) if first_buy.get("是否底背驰") else {}
            results.append({
                "股票代码": code,
                "级别": level,
                "信号类型": "第一买点",
                "当前价格": round(current_price, 2),
                "一买位置": first_buy.get("一买位置"),
                "是否底背驰": "是" if first_buy.get("是否底背驰") else "否",
                "均线金叉": "是" if first_buy.get("均线金叉") else "否",
                "力度评分": div_strength.get("力度评分", 0),
                "预估上涨概率": div_strength.get("预估上涨概率", ""),
                "预估上涨力度": div_strength.get("预估上涨力度", ""),
                "原因": "; ".join(first_buy.get("原因", [])),
            })

    return results


def run(limit: int = None, levels: List[str] = None, output_csv: str = None):
    """运行分析"""
    if levels is None:
        levels = ["daily"]

    print(f"=== 缠论买卖点分析 ({datetime.now().strftime('%Y-%m-%d')}) ===")

    # 生成股票列表
    codes = generate_stock_list(limit)
    print(f"待分析股票: {len(codes)} 只")
    print(f"分析级别: {', '.join(levels)}")

    # 获取股票名称
    names = get_stock_names(codes)

    # 分析每只股票
    all_results = []
    for i, code in enumerate(codes):
        if (i + 1) % 500 == 0 or i == 0:
            print(f"分析进度: {i+1}/{len(codes)}")

        results = analyze_stock(code, levels)
        for r in results:
            r["股票名称"] = names.get(code, code)
            all_results.append(r)

    # 创建DataFrame
    df = pd.DataFrame(all_results)

    if not df.empty:
        # 排序：第三买点 > 第二买点 > 第一买点
        signal_order = {"第三买点": 3, "第二买点": 2, "第一买点": 1}
        df["排序"] = df["信号类型"].map(signal_order)

        # 二买按综合评分排序，一买按力度评分排序
        if "综合评分" in df.columns:
            # 合并评分列
            df["排序评分"] = df.apply(
                lambda x: x.get("力度评分", 0) if x["信号类型"] == "第一买点" else x.get("综合评分", 0),
                axis=1
            )
            df = df.sort_values(by=["排序", "排序评分"], ascending=[False, False])
        else:
            df = df.sort_values(by=["排序", "力度评分" if "力度评分" in df.columns else "当前价格"],
                              ascending=[False, False])
        df = df.drop(columns=["排序", "排序评分"] if "排序评分" in df.columns else ["排序"])

        # 添加级别说明
        level_names = {"daily": "日线", "60": "60分钟", "30": "30分钟", "15": "15分钟", "5": "5分钟"}
        df["级别说明"] = df["级别"].map(lambda x: level_names.get(x, x))

    # 输出统计
    print(f"\n=== 分析结果 ===")
    if not df.empty:
        signal_counts = df["信号类型"].value_counts()
        print(f"第三买点: {signal_counts.get('第三买点', 0)} 只")
        print(f"第二买点: {signal_counts.get('第二买点', 0)} 只")
        print(f"第一买点: {signal_counts.get('第一买点', 0)} 只")
    else:
        print("未找到符合条件的股票")

    # 保存CSV
    if output_csv:
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存: {output_csv}")

    if not df.empty:
        print(f"\n=== 信号股票列表 ===")
        print(df.to_string(index=False))

    return df


def main():
    parser = argparse.ArgumentParser(description="缠论买卖点分析")
    parser.add_argument("--limit", type=int, default=None, help="分析前N只股票")
    parser.add_argument("--levels", type=str, default="daily", help="分析级别，逗号分隔")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出CSV路径")
    args = parser.parse_args()

    levels = [l.strip() for l in args.levels.split(",")] if args.levels else ["daily"]
    output = args.output or f"buy_signals_{datetime.now().strftime('%Y%m%d')}.csv"

    start_time = time.time()
    run(limit=args.limit, levels=levels, output_csv=output)
    elapsed = time.time() - start_time
    print(f"\n总耗时: {elapsed:.1f}秒 ({elapsed/60:.1f}分钟)")


if __name__ == "__main__":
    main()
