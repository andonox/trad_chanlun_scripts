# -*- coding: utf-8 -*-
"""
MiniMax API 分析模块
"""
import os
import json
import requests
from typing import Dict, List, Optional


# 读取环境变量
MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.getenv("MINIMAX_BASE_URL", "https://api.minimax.chat/v1")


def get_minimax_api_key() -> str:
    """获取API Key"""
    if MINIMAX_API_KEY:
        return MINIMAX_API_KEY

    # 尝试从.env文件读取
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith("MINIMAX_API_KEY="):
                    return line.split("=", 1)[1].strip()

    return ""


def call_minimax(prompt: str, system_prompt: str = None) -> str:
    """调用MiniMax API"""
    api_key = get_minimax_api_key()

    if not api_key:
        return "错误: 未配置MINIMAX_API_KEY"

    url = f"{MINIMAX_BASE_URL}/text/chatcompletion_v2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    data = {
        "model": "abab6.5s-chat",
        "messages": messages
    }

    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        result = response.json()

        if "choices" in result and len(result["choices"]) > 0:
            return result["choices"][0]["message"]["content"]
        elif "error" in result:
            return "API错误: " + str(result["error"])
        else:
            return "未知错误: " + str(result)

    except Exception as e:
        return "请求失败: " + str(e)


def analyze_stock(code: str, name: str, signal_type: str, level: str,
                 zg: float = None, distance_pct: float = None) -> str:
    """分析单个股票"""
    api_key = get_minimax_api_key()
    if not api_key:
        return "未配置MINIMAX_API_KEY"

    system_prompt = """你是一位专业的A股缠论技术分析助手。根据提供的股票技术指标，给出专业的分析结论和操作建议。"""

    prompt = """请分析以下股票的技术面情况：

股票代码: {}
股票名称: {}
信号类型: {} ({})
级别: {}

""".format(code, name, signal_type, "第三买点" if signal_type == "第三买点" else "底背驰", level)

    if zg is not None:
        prompt += "中枢上沿(ZG): {:.2f}\n".format(zg)
    if distance_pct is not None:
        prompt += "距ZG百分比: {:.2f}%\n".format(distance_pct)

    prompt += """
请给出：
1. 当前位置的技术分析
2. 后续走势预判
3. 操作建议（买入/持有/卖出）

请用简洁专业的语言回答。"""

    return call_minimax(prompt, system_prompt)


def analyze_batch(stocks: List[Dict]) -> List[Dict]:
    """批量分析股票"""
    api_key = get_minimax_api_key()
    if not api_key:
        return [{"error": "未配置MINIMAX_API_KEY"}]

    results = []

    for stock in stocks:
        code = stock.get("股票代码", "")
        name = stock.get("股票名称", "")
        signal_type = stock.get("类型", stock.get("信号类型", ""))
        level = stock.get("级别说明", stock.get("级别", ""))
        zg = stock.get("ZG")
        distance_pct = stock.get("距ZG百分比")

        print("分析 {} {}...".format(code, name))

        analysis = analyze_stock(code, name, signal_type, level, zg, distance_pct)

        results.append({
            "股票代码": code,
            "股票名称": name,
            "信号类型": signal_type,
            "级别": level,
            "分析结论": analysis
        })

    return results


def save_analysis_results(results: List[Dict], output_csv: str):
    """保存分析结果到CSV"""
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print("分析结果已保存: {}".format(output_csv))


# 为了避免循环导入，在函数内导入pandas
import pandas as pd
