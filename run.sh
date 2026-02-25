#!/bin/bash
# 缠论第三买点筛选 - Tushare版 快速启动脚本

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${SCRIPT_DIR}/venv/bin/python"

# 检查虚拟环境
if [ ! -f "$PYTHON" ]; then
    PYTHON="python"
fi

echo "========================================"
echo "  缠论第三买点筛选 - Tushare版"
echo "========================================"
echo ""
echo "请选择运行模式:"
echo "  [1] 快速测试 (20只股票)"
echo "  [2] 中等批量 (500只股票)"
echo "  [3] 全量A股 (~5000只)"
echo "  [4] 仅分析已有数据 (跳过下载)"
echo "  [5] 自定义参数"
echo ""
read -p "请输入选项 [1-5]: " mode

case $mode in
    1)
        echo ""
        echo "运行: 快速测试 20只股票..."
        "$PYTHON" "$SCRIPT_DIR/main_tushare.py" --limit 20 --delay 0.1
        ;;
    2)
        echo ""
        echo "运行: 500只股票..."
        "$PYTHON" "$SCRIPT_DIR/main_tushare.py" --limit 500 --delay 0.1
        ;;
    3)
        echo ""
        echo "运行: 全量A股 (~5000只)..."
        echo "这可能需要几分钟..."
        "$PYTHON" "$SCRIPT_DIR/main_tushare.py" --limit 5000 --delay 0.05
        ;;
    4)
        echo ""
        echo "运行: 仅分析已有数据..."
        read -p "请输入股票代码(逗号分隔): " codes
        if [ -n "$codes" ]; then
            "$PYTHON" "$SCRIPT_DIR/main_tushare.py" --codes "$codes" --skip-fetch
        else
            echo "错误: 请输入股票代码"
        fi
        ;;
    5)
        echo ""
        echo "请输入参数 (直接回车使用默认值):"
        read -p "股票数量 (默认500): " limit
        read -p "月数 (默认1): " months
        read -p "请求间隔秒 (默认0.1): " delay

        CMD="$PYTHON $SCRIPT_DIR/main_tushare.py"
        [ -n "$limit" ] && [ "$limit" != "" ] && CMD="$CMD --limit $limit"
        [ -n "$months" ] && [ "$months" != "" ] && CMD="$CMD --months $months"
        [ -n "$delay" ] && [ "$delay" != "" ] && CMD="$CMD --delay $delay"

        echo ""
        echo "运行: $CMD"
        eval $CMD
        ;;
    *)
        echo "无效选项"
        ;;
esac

echo ""
echo "========================================"
echo "完成!"
