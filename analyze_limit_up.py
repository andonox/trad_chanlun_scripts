# -*- coding: utf-8 -*-
"""
涨停板分析脚本
分析当天涨停的股票，看前一天是否有底背驰
"""
import sys
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import argparse
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd

from kline_db import get_connection, init_db
from chan_logic import check_bottom_divergence
from data_fetcher_tushare import get_kline_by_level


def get_stock_names(codes: List[str]) -> Dict[str, str]:
    """获取股票名称"""
    try:
        import tushare as ts
        from config import TUSHARE_TOKEN
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()

        df = pro.stock_basic(ts_code=",".join([f"{c}.SH" if c.startswith('6') else f"{c}.SZ" for c in codes]),
                            fields='ts_code,name')
        return dict(zip([str(x['ts_code'])[:6] for x in df.to_dict('records')],
                      [x['name'] for x in df.to_dict('records')]))
    except Exception as e:
        print("获取股票名称失败: {}".format(e))
        return {}


def get_limit_up_stocks(trade_date: str = None) -> List[str]:
    """获取当天涨停的股票"""
    try:
        import tushare as ts
        from config import TUSHARE_TOKEN
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")

        # 获取所有股票数据
        df = pro.daily(trade_date=trade_date)
        if df is None or df.empty:
            print("无法获取数据")
            return []

        # 根据涨跌幅判断涨停（A股涨跌停10%）
        # 过滤涨幅>=9.9%的（近似涨停）
        limit_up = df[df['pct_chg'] >= 9.5]
        codes = [str(x['ts_code'])[:6] for x in limit_up.to_dict('records')]
        return codes
    except Exception as e:
        print("获取涨停股票失败: {}".format(e))
        return []


def check_prev_day_bottom_divergence(code: str) -> Dict:
    """检查前一天是否有底背驰"""
    conn = get_connection()
    try:
        init_db(conn)

        # 获取前一天的数据
        yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

        # 读取日线数据
        df = pd.read_sql(
            "SELECT * FROM kline WHERE symbol=? AND level='daily' AND dt < ? ORDER BY dt DESC LIMIT 30",
            conn, params=(code, datetime.now().strftime("%Y-%m-%d"))
        )

        if df is None or len(df) < 20:
            return {"有底背驰": False, "原因": "数据不足"}

        # 检查底背驰（用前一天的数据）
        div_result = check_bottom_divergence(df)

        return {
            "有底背驰": div_result.get("存在底背驰", False),
            "价格新低": div_result.get("价格新低"),
            "原因": div_result.get("原因", [])
        }
    except Exception as e:
        return {"有底背驰": False, "原因": str(e)}
    finally:
        conn.close()


def run(output_csv: str = None, trade_date: str = None):
    """运行涨停板分析"""
    print("=" * 60)
    print("涨停板底背驰分析")
    print("=" * 60)

    # 获取当天涨停股票
    if trade_date:
        limit_up_codes = get_limit_up_stocks(trade_date)
    else:
        limit_up_codes = get_limit_up_stocks()

    if not limit_up_codes:
        print("未找到涨停股票")
        return pd.DataFrame()

    print("当天涨停股票数: {}".format(len(limit_up_codes)))

    # 获取股票名称
    names = get_stock_names(limit_up_codes)

    # 检查每个股票前一天是否有底背驰
    results = []
    for i, code in enumerate(limit_up_codes):
        if (i + 1) % 20 == 0:
            print("分析进度: {}/{}".format(i+1, len(limit_up_codes)))

        name = names.get(code, code)

        # 检查底背驰
        div = check_prev_day_bottom_divergence(code)

        results.append({
            "股票代码": code,
            "股票名称": name,
            "前一天底背驰": "是" if div.get("有底背驰") else "否",
            "价格新低": div.get("价格新低"),
            "原因": "; ".join(div.get("原因", []))
        })

        if div.get("有底背驰"):
            print("  {} {}: 前一天底背驰!".format(code, name))

    # 创建DataFrame
    df = pd.DataFrame(results)

    # 筛选出前一天有底背驰的
    df_zhangting = df[df["前一天底背驰"] == "是"]

    print("")
    print("=" * 60)
    print("分析结果")
    print("=" * 60)
    print("当天涨停: {} 只".format(len(df)))
    print("前一天有底背驰: {} 只".format(len(df_zhangting)))

    if not df_zhangting.empty:
        print("")
        print("重点关注（涨停+前一天底背驰）:")
        print(df_zhangting[["股票代码", "股票名称", "价格新低"]].to_string(index=False))

    # 保存CSV
    if output_csv:
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print("\n结果已保存: {}".format(output_csv))

    return df


def main():
    parser = argparse.ArgumentParser(description="涨停板底背驰分析")
    parser.add_argument("--date", type=str, default=None, help="分析日期(YYYYMMDD)，默认为今天")
    parser.add_argument("--output", "-o", type=str, default=None, help="输出CSV路径")
    args = parser.parse_args()

    output = args.output or "limit_up_divergence_{}.csv".format(
        datetime.now().strftime("%Y%m%d")
    )

    run(output_csv=output, trade_date=args.date)


if __name__ == "__main__":
    main()
