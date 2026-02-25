@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PYTHON=C:\ProgramData\anaconda3\envs\trad\python.exe"

echo ========================================
echo  缠论第三买点筛选 - Tushare版
echo ========================================
echo.
echo 请选择运行模式:
echo   [1] 快速测试 (20只股票)
echo   [2] 中等批量 (500只股票)
echo   [3] 全量A股 (~5000只)
echo   [4] 仅分析已有数据 (跳过下载)
echo   [5] 自定义参数
echo.
set /p mode="请输入选项 [1-5]: "

if "%mode%"=="1" (
    echo.
    echo 运行: 快速测试 20只股票...
    "%PYTHON%" "%SCRIPT_DIR%main_tushare.py" --limit 20 --delay 0.1
    goto end
)

if "%mode%"=="2" (
    echo.
    echo 运行: 500只股票...
    "%PYTHON%" "%SCRIPT_DIR%main_tushare.py" --limit 500 --delay 0.1
    goto end
)

if "%mode%"=="3" (
    echo.
    echo 运行: 全量A股 (~5000只)...
    echo 这可能需要几分钟...
    "%PYTHON%" "%SCRIPT_DIR%main_tushare.py" --limit 5000 --delay 0.05
    goto end
)

if "%mode%"=="4" (
    echo.
    echo 运行: 仅分析已有数据...
    set /p codes="请输入股票代码(逗号分隔): "
    if defined codes (
        "%PYTHON%" "%SCRIPT_DIR%main_tushare.py" --codes "%codes%" --skip-fetch
    ) else (
        echo 错误: 请输入股票代码
    )
    goto end
)

if "%mode%"=="5" (
    echo.
    echo 请输入参数 (直接回车使用默认值):
    set /p limit="股票数量 (默认500): "
    set /p months="月数 (默认1): "
    set /p delay="请求间隔秒 (默认0.1): "

    set "CMD=%PYTHON% %SCRIPT_DIR%main_tushare.py"

    if defined limit if not "%limit%"=="" set "CMD=%CMD% --limit %limit%"
    if defined months if not "%months%"=="" set "CMD=%CMD% --months %months%"
    if defined delay if not "%delay%"=="" set "CMD=%CMD% --delay %delay%"

    echo.
    echo 运行: !CMD!
    !CMD!
    goto end
)

echo 无效选项
goto end

:end
echo.
echo ========================================
echo 完成! 按任意键退出...
pause >nul
