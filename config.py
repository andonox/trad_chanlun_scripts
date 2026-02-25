# -*- coding: utf-8 -*-
"""配置文件：读取环境变量"""
import os
from pathlib import Path

# 获取项目根目录
PROJECT_ROOT = Path(__file__).parent

# 读取.env文件
def load_env():
    """加载.env配置文件"""
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")  # 去除引号
                    os.environ[key] = value

# 加载环境变量
load_env()

# Tushare配置
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")
