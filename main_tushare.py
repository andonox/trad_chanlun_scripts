# -*- coding: utf-8 -*-
"""
A股缠论第三买点筛选脚本 - Tushare数据源版本
采用两阶段模式：
1. 批量获取所有股票数据
2. 从本地数据库读取并分析
"""
import sys
import io
import os

# 设置控制台编码
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from data_fetcher_tushare import (
    LEVELS,
    get_kline_by_level,
    batch_fetch_stocks,
)
from chan_logic import filter_levels_third_buy, filter_levels_third_buy_with_detail, check_bottom_divergence, calculate_divergence_strength
from kline_db import checkpoint as db_checkpoint, get_connection, init_db


def generate_stock_list(limit: Optional[int] = None) -> pd.DataFrame:
    """生成A股股票列表"""
    codes = []

    # 沪市主板 600000-603999 (约400只)
    codes.extend([f"{i:06d}" for i in range(600000, 604000)])

    # 深市主板 000001-000999, 001000-001999 (约2000只)
    codes.extend([f"{i:06d}" for i in range(1, 1000)])
    codes.extend([f"{i:06d}" for i in range(1000, 2000)])

    # 创业板 300001-300999 (约1000只)
    codes.extend([f"{i:06d}" for i in range(300001, 301000)])

    if limit:
        codes = codes[:limit]

    df = pd.DataFrame({'代码': codes})
    df['名称'] = df['代码']
    return df


def get_stock_names(codes: List[str]) -> Dict[str, str]:
    """从Tushare获取股票名称"""
    try:
        import tushare as ts
        from config import TUSHARE_TOKEN
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()

        # 一次性获取所有上市股票
        df = pro.stock_basic(list_status='L', fields='ts_code,name')
        if df is None or df.empty:
            return {}

        return dict(zip([str(x['ts_code'])[:6] for x in df.to_dict('records')],
                      [x['name'] for x in df.to_dict('records')]))
    except Exception as e:
        print("获取股票名称失败: {}".format(e))
        return {}


def get_stock_list(limit: Optional[int] = None) -> pd.DataFrame:
    """获取A股股票列表（代码、名称）"""
    df = generate_stock_list(limit)

    # 尝试获取股票名称
    names = get_stock_names(df['代码'].tolist())

    if names:
        df['名称'] = df['代码'].map(lambda x: names.get(x, x))

    return df


def level_display_name(level: str) -> str:
    """级别显示名称。"""
    names = {"daily": "日线", "60": "60分钟", "30": "30分钟", "15": "15分钟", "5": "5分钟"}
    return names.get(level, level)


def _setup_logging(log_dir: Optional[str] = None) -> Optional[str]:
    """配置 root logger"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = None
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"third_buy_{ts}.log")
        file_h = logging.FileHandler(log_path, encoding="utf-8")
        file_h.setLevel(logging.DEBUG)
        file_h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logging.root.addHandler(file_h)
    return log_path


def run(
    months: int = 1,
    limit_stocks: Optional[int] = None,
    stock_codes: Optional[List[str]] = None,
    levels: Optional[List[str]] = None,
    output_csv: Optional[str] = None,
    request_delay: float = 0.1,
    log_dir: Optional[str] = None,
    workers: int = 10,
    skip_fetch: bool = False,
) -> pd.DataFrame:
    """两阶段执行"""
    if levels is None:
        levels = ["daily"]  # 默认只分析日线

    log_path = _setup_logging(log_dir) if log_dir else None

    # 获取股票列表
    if stock_codes:
        codes = [str(c).strip().zfill(6) for c in stock_codes]
        stock_list = pd.DataFrame([{"代码": c, "名称": c} for c in codes])
    else:
        stock_list = get_stock_list(limit=limit_stocks)
        if stock_list.empty:
            print("无法获取股票列表")
            return pd.DataFrame(columns=["股票代码", "股票名称", "级别", "级别说明"])

    total_stocks = len(stock_list)
    print(f"共 {total_stocks} 只股票待处理")

    # 阶段1: 批量获取数据
    if not skip_fetch:
        codes_to_fetch = stock_list["代码"].tolist()
        print(f"\n========== 阶段1: 批量获取数据 ==========")
        batch_fetch_stocks(codes_to_fetch, levels=levels, delay=request_delay)

        # checkpoint
        try:
            busy, log_frames, checkpointed = db_checkpoint()
            print(f"数据库 checkpoint: 合并 {checkpointed} 页")
        except Exception as e:
            print(f"checkpoint 异常: {e}")

    # 阶段2: 分析筛选
    print(f"\n========== 阶段2: 分析筛选 ==========")
    results = []
    for i, (_, row) in enumerate(stock_list.iterrows()):
        code = str(row["代码"]).strip()
        name = str(row["名称"]).strip()

        if (i + 1) % 500 == 0 or i == 0:
            print(f"分析进度: {i+1}/{total_stocks}")

        # 读取数据
        kline_by_level = {}
        for lev in levels:
            kline_by_level[lev] = get_kline_by_level(code, lev, months=months)

        # 分析第三买点
        detail_list = filter_levels_third_buy_with_detail(kline_by_level, levels)
        third_buy_levels = [lev for lev, _, passed in detail_list if passed]

        # 分析底背驰
        bottom_div_levels = []
        div_scores = {}
        for lev in levels:
            df = kline_by_level.get(lev)
            if df is not None:
                # 计算底背驰力度
                strength = calculate_divergence_strength(df)
                if strength.get("有底背驰"):
                    bottom_div_levels.append(lev)
                    div_scores[lev] = strength

        # 记录第三买点
        for lev in third_buy_levels:
            results.append({
                "股票代码": code,
                "股票名称": name,
                "级别": lev,
                "级别说明": level_display_name(lev),
                "类型": "第三买点",
                "力度评分": 100,
                "预估上涨概率": "极高",
                "预估上涨力度": "强劲",
            })

        # 记录底背驰（不重复）
        existing_codes = set((r["股票代码"], r["级别"]) for r in results)
        for lev in bottom_div_levels:
            if (code, lev) not in existing_codes:
                strength = div_scores.get(lev, {})
                results.append({
                    "股票代码": code,
                    "股票名称": name,
                    "级别": lev,
                    "级别说明": level_display_name(lev),
                    "类型": "底背驰",
                    "力度评分": strength.get("力度评分", 0),
                    "预估上涨概率": strength.get("预估上涨概率", ""),
                    "预估上涨力度": strength.get("预估上涨力度", ""),
                })

        if third_buy_levels or bottom_div_levels:
            info = []
            if third_buy_levels:
                info.append("3买:" + ",".join(level_display_name(l) for l in third_buy_levels))
            if bottom_div_levels:
                info.append("底背:" + ",".join(level_display_name(l) for l in bottom_div_levels))
            print(f"  {code} 符合: {', '.join(info)}")

    # 输出结果
    out_df = pd.DataFrame(results)

    # 按力度评分排序
    if not out_df.empty:
        # 确保评分列是数值类型
        out_df["力度评分"] = pd.to_numeric(out_df["力度评分"], errors='coerce').fillna(0)
        # 排序：第三买点在先，然后按评分降序
        out_df["排序权重"] = out_df["类型"].apply(lambda x: 1 if x == "第三买点" else 0)
        out_df = out_df.sort_values(by=["排序权重", "力度评分"], ascending=[False, False])
        out_df = out_df.drop(columns=["排序权重"])

    if output_csv:
        out_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存: {output_csv}")

    print(f"\n共筛选出 {len(out_df)} 条记录")
    if not out_df.empty:
        print(out_df.to_string(index=False))

    return out_df


def main():
    parser = argparse.ArgumentParser(description="A股缠论第三买点筛选 - Tushare版")
    parser.add_argument("--months", type=int, default=1, help="日线月数")
    parser.add_argument("--limit", type=int, default=None, help="处理前N只股票")
    parser.add_argument("--codes", type=str, default=None, help="指定股票代码，逗号分隔")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出CSV路径")
    parser.add_argument("--delay", type=float, default=0.1, help="请求间隔秒数")
    parser.add_argument("--workers", type=int, default=10, help="并发线程数")
    parser.add_argument("--log-dir", type=str, default=None, help="日志目录")
    parser.add_argument("--skip-fetch", action="store_true", help="跳过数据获取，仅分析已有数据")
    parser.add_argument("--levels", type=str, default="daily", help="分析的级别")
    args = parser.parse_args()

    # 解析级别
    levels = [l.strip() for l in args.levels.split(",")] if args.levels else ["daily"]

    stock_codes = None
    if args.codes:
        stock_codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    default_output = f"signal_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    output = args.output or default_output

    start_time = time.time()

    df = run(
        months=args.months,
        limit_stocks=args.limit,
        stock_codes=stock_codes,
        levels=levels,
        output_csv=output,
        request_delay=args.delay,
        log_dir=args.log_dir,
        workers=args.workers,
        skip_fetch=args.skip_fetch,
    )

    elapsed = time.time() - start_time
    print(f"\n总耗时: {elapsed:.1f}秒 ({elapsed/60:.1f}分钟)")


if __name__ == "__main__":
    main()
