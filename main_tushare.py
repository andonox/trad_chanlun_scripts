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
from chan_logic import filter_levels_third_buy, filter_levels_third_buy_with_detail
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


def get_stock_list(limit: Optional[int] = None) -> pd.DataFrame:
    """获取A股股票列表（代码、名称）"""
    return generate_stock_list(limit)


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

        # 分析
        detail_list = filter_levels_third_buy_with_detail(kline_by_level, levels)
        hit_levels = [lev for lev, _, passed in detail_list if passed]

        if hit_levels:
            for lev in hit_levels:
                results.append({
                    "股票代码": code,
                    "股票名称": name,
                    "级别": lev,
                    "级别说明": level_display_name(lev),
                })
            print(f"  {code} 符合: {', '.join(level_display_name(l) for l in hit_levels)}")

    # 输出结果
    out_df = pd.DataFrame(results)
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

    default_output = f"third_buy_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
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
