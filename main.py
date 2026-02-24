# -*- coding: utf-8 -*-
"""
A股缠论第三买点筛选脚本。
获取至少1个月多级别K线与MACD，筛选当日各级别符合缠论第三买点的股票，输出代码、名称与级别。
支持详细 log（检测项、条件满足/不满足）写入 log 文件，以及通过 --codes 指定股票代码。
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from data_fetcher import (
    LEVELS,
    get_kline_by_level,
)
from chan_logic import filter_levels_third_buy, filter_levels_third_buy_with_detail
from kline_db import checkpoint as db_checkpoint


def _normalize_stock_list_df(df: pd.DataFrame, limit: Optional[int]) -> pd.DataFrame:
    """统一为 代码/名称 两列并过滤。"""
    if df is None or df.empty:
        return pd.DataFrame()
    # 兼容不同接口列名
    if "code" in df.columns and "name" in df.columns:
        df = df.rename(columns={"code": "代码", "name": "名称"})
    if "代码" not in df.columns or "名称" not in df.columns:
        return pd.DataFrame()
    df = df[["代码", "名称"]].drop_duplicates(subset=["代码"]).dropna(subset=["代码", "名称"])
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df = df[~df["代码"].str.startswith("8")]  # 过滤北交所
    if limit:
        df = df.head(limit)
    return df.reset_index(drop=True)


def get_stock_list(limit: Optional[int] = None, retries: int = 3, retry_delay: float = 3.0) -> pd.DataFrame:
    """获取 A 股股票列表（代码、名称）。带重试与备用接口。"""
    import akshare as ak
    # 方案1：先用 stock_info_a_code_name（通常更稳定，不依赖行情页）
    for attempt in range(retries):
        try:
            df = ak.stock_info_a_code_name()
            out = _normalize_stock_list_df(df, limit)
            if not out.empty:
                return out
        except Exception as e:
            if attempt < retries - 1:
                print(f"  stock_info_a_code_name 第 {attempt + 1} 次失败: {e}，{retry_delay} 秒后重试...", file=sys.stderr)
                time.sleep(retry_delay)
            else:
                pass  # 最后再试 spot
    # 方案2：备用 实时行情列表
    for attempt in range(retries):
        try:
            df = ak.stock_zh_a_spot_em()
            out = _normalize_stock_list_df(df, limit)
            if not out.empty:
                return out
        except Exception as e:
            if attempt < retries - 1:
                print(f"  stock_zh_a_spot_em 第 {attempt + 1} 次失败: {e}，{retry_delay} 秒后重试...", file=sys.stderr)
                time.sleep(retry_delay)
            else:
                print(f"获取股票列表失败: {e}", file=sys.stderr)
    return pd.DataFrame()


def level_display_name(level: str) -> str:
    """级别显示名称。"""
    names = {"daily": "日线", "60": "60分钟", "30": "30分钟", "15": "15分钟", "5": "5分钟", "1": "1分钟"}
    return names.get(level, level)


def _setup_logging(log_dir: Optional[str] = None) -> Optional[str]:
    """配置 root logger：控制台 INFO；若 log_dir 指定则同时写入带时间戳的 log 文件。返回 log 文件路径。"""
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
    request_delay: float = 0.5,
    log_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    主流程：拉取股票列表（或仅指定代码），逐只获取多级别 K 线，筛选第三买点，汇总结果。
    stock_codes: 若指定，只检测这些代码（仍会拉全表解析名称，缺失名称的用代码代替）。
    log_dir: 若指定，在该目录下写带时间戳的 log 文件，记录检测项与条件满足/不满足。
    """
    if levels is None:
        levels = LEVELS

    log_path = None
    if log_dir is not None:
        log_path = _setup_logging(log_dir)
        if log_path:
            logging.info("日志文件: %s", log_path)

    stock_list = get_stock_list(limit=limit_stocks if stock_codes is None else None)
    if stock_codes is not None:
        codes_set = {str(c).strip().zfill(6) for c in stock_codes}
        if not stock_list.empty:
            stock_list = stock_list[stock_list["代码"].astype(str).str.zfill(6).isin(codes_set)]
        else:
            stock_list = pd.DataFrame([{"代码": c, "名称": c} for c in codes_set])
        if stock_list.empty:
            logging.warning("指定代码未在列表中找到，使用代码作为名称: %s", list(codes_set))
            stock_list = pd.DataFrame([{"代码": c, "名称": c} for c in sorted(codes_set)])
    if stock_list.empty:
        return pd.DataFrame(columns=["股票代码", "股票名称", "级别", "级别说明"])

    results = []
    total = len(stock_list)
    for i, (_, row) in enumerate(stock_list.iterrows()):
        if i > 0 and request_delay > 0:
            time.sleep(request_delay)
        code = str(row["代码"]).strip()
        name = str(row["名称"]).strip()
        if len(code) < 6:
            code = code.zfill(6)
        msg_head = f"[{i+1}/{total}] 检查 {code} {name}"
        print(f"{msg_head} ...", end=" ", flush=True)
        if log_path:
            logging.info("======== 检测 %s %s ========", code, name)

        kline_by_level = {}
        for lev in levels:
            kline_by_level[lev] = get_kline_by_level(code, lev, months=months)
        detail_list = filter_levels_third_buy_with_detail(kline_by_level, levels)
        hit_levels = [lev for lev, _, passed in detail_list if passed]

        for lev, detail, passed in detail_list:
            lev_name = level_display_name(lev)
            if log_path:
                logging.info("  级别 %s: 中枢存在=%s, ZD=%s, ZG=%s, 曾突破ZG=%s, 回抽不破ZG=%s, 当前收盘在ZG上=%s -> 结论=%s",
                    lev_name,
                    detail.get("中枢存在"),
                    detail.get("ZD"),
                    detail.get("ZG"),
                    detail.get("曾突破ZG"),
                    detail.get("回抽不破ZG"),
                    detail.get("当前收盘在ZG上"),
                    "符合" if passed else "不符合",
                )
                if detail.get("原因"):
                    for r in detail["原因"]:
                        logging.info("    -> %s", r)

        if hit_levels:
            for lev in hit_levels:
                results.append({
                    "股票代码": code,
                    "股票名称": name,
                    "级别": lev,
                    "级别说明": level_display_name(lev),
                })
            print(" 符合级别:", ", ".join(level_display_name(l) for l in hit_levels))
            if log_path:
                logging.info("  => 符合级别: %s", ", ".join(level_display_name(l) for l in hit_levels))
        else:
            print(" 无")
            if log_path:
                logging.info("  => 无符合级别")

    out_df = pd.DataFrame(results)
    if output_csv:
        out_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"\n结果已保存: {output_csv}")
        if log_path:
            logging.info("结果CSV: %s", output_csv)
    if log_path:
        logging.info("共筛选出 %d 条记录", len(out_df))
    # 将 WAL 合并回主库，使 stock_kline.db 文件反映全部数据，-wal/-shm 可被截断
    try:
        busy, log_frames, checkpointed = db_checkpoint()
        if log_path:
            logging.info("DB checkpoint: busy=%s, log_frames=%s, checkpointed_frames=%s", busy, log_frames, checkpointed)
        if checkpointed > 0 or log_frames > 0:
            print(f"\n数据库已合并 WAL 到主库: 本次合并 {checkpointed} 页 (WAL 中共 {log_frames} 页)")
        else:
            print("\n数据库 checkpoint: WAL 为空，主库未变化（本次运行未写入新数据，可能数据已是最新）")
    except Exception as e:
        print(f"\n数据库 checkpoint 异常: {e}")
        if log_path:
            logging.warning("数据库 checkpoint 未执行: %s", e)
    return out_df


def main():
    parser = argparse.ArgumentParser(description="A股缠论第三买点筛选")
    parser.add_argument("--months", type=int, default=1, help="至少获取的月数（日线）")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 只股票（测试用）")
    parser.add_argument("--codes", type=str, default=None, help="仅检测指定股票代码，逗号分隔，如 000001,600519")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出 CSV 路径")
    parser.add_argument("--delay", type=float, default=0.5, help="每只股票请求间隔秒数，防断连（默认0.5）")
    parser.add_argument("--log-dir", type=str, default="log", help="日志目录，写入详细检测与条件结果（默认 log）")
    args = parser.parse_args()

    stock_codes = None
    if args.codes:
        stock_codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    default_output = f"third_buy_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    output = args.output or default_output
    log_dir = (args.log_dir or "").strip() or None  # 空字符串表示不写 log 文件
    df = run(
        months=args.months,
        limit_stocks=args.limit,
        stock_codes=stock_codes,
        output_csv=output,
        request_delay=args.delay,
        log_dir=log_dir,
    )
    print("\n共筛选出", len(df), "条记录（同一只股票多级别会多行）")
    if not df.empty:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
