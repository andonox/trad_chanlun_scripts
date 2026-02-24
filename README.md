# A股缠论第三买点筛选

获取至少 1 个月 A 股多级别 K 线及 MACD，筛选**当日**各级别符合缠论第三买点的股票，输出股票代码、名称及对应级别。

## 环境

- Python 3.8+
- 依赖：`akshare`、`pandas`、`numpy`

安装依赖：

```bash
pip install -r requirements.txt
```

## 用法

推荐使用 conda 的 `trad` 环境运行：

```bash
cd e:\trading_data\find3rdbuypoint
conda activate trad
python main.py --limit 20 --delay 1
```

或直接指定环境（不激活）：

```bash
conda run -n trad python main.py --limit 20 --delay 1
```

```bash
# 全市场扫描（耗时较长），结果保存到 CSV
python main.py

# 至少使用 2 个月日线数据
python main.py --months 2

# 仅跑前 20 只股票做测试（建议加 --delay 1 降低断连概率）
python main.py --limit 20 --delay 1

# 仅检测指定股票代码（逗号分隔）
python main.py --codes 000001,600519,000858

# 写详细 log 到目录（默认 log 目录，文件名 third_buy_YYYYMMDD_HHMMSS.log）
python main.py --log-dir log
# 不写 log 文件
python main.py --log-dir ""

# 指定输出文件
python main.py -o result.csv
```

## 输出说明

- 终端会逐只打印检查进度；符合第三买点的会列出「符合级别」。
- 最终结果表列：**股票代码**、**股票名称**、**级别**、**级别说明**。
- 同一只股票若在多个级别符合，会占多行（每行一个级别）。
- 默认会生成带时间戳的 CSV：`third_buy_YYYYMMDD_HHMM.csv`。
- **日志**：使用默认 `--log-dir log` 时，会在 `log/` 下生成 `third_buy_YYYYMMDD_HHMMSS.log`，记录每只股票的检测项、各级别条件（中枢存在、ZD/ZG、曾突破ZG、回抽不破ZG、当前收盘在ZG上）是否满足及不满足原因，以及最终是否符合。

## 数据存储与增量更新

- **数据库**：K 线（含 MACD）存在项目目录下 **`data/stock_kline.db`**（SQLite）。
- **读取**：脚本**优先从数据库取**该股票、该级别的数据。
- **是否最新**：
  - 日线：若库内最新日期不早于「今天−4 天」（覆盖周末），视为已最新，不再请求接口。
  - 分钟：若库内最新一条的日期是今天，视为已最新。
- **增量拉取**：若判定不是最新，则只拉「库内最新日期之后」到「当前」的数据（日线），或最近几天（分钟），**写入数据库后再从库中读出**返回，从而减少请求量和耗时。
- 首次运行或新股票会做一次全量/多日拉取并写入，之后每次只补到最新。
- 调出来的数据统一从数据库取，保证与写入一致。
- **关于 stock_kline.db 很小、只看到 -wal/-shm 在变**：库使用了 SQLite 的 **WAL 模式**。写入会先进入 `stock_kline.db-wal`，主文件 `stock_kline.db` 要等 **checkpoint** 才会把 WAL 里的改动合并进去。脚本**每次跑完**会执行一次 checkpoint 并打印结果（合并了多少页）。  
- **若主库修改时间/大小都没变**：多半是**本次运行没有新写入**（库里数据已是最新，只读了没写）。此时 WAL 为空，checkpoint 无页可合并，主库不会变。可先看终端最后一行：若为「WAL 为空，主库未变化（本次运行未写入新数据）」即属此类。若要验证合并逻辑：删除 `data/` 目录后重跑（会全量拉取并写入），跑完应看到「已合并 N 页」且主库变大。

## 级别

- **日线**：至少 1 个月（可 `--months` 调大）。
- **60/30/15/5 分钟**：依赖数据源，一般为近期约 5～10 个交易日。

## 缠论第三买点（本脚本采用简化规则）

- **中枢**：最近若干根 K 线按三段划分，取三段高低点的重叠区间为中枢 [ZD, ZG]。
- **第三买点**：价格向上突破中枢上沿 ZG 后回抽，回抽低点不跌破 ZG，且当前收盘在 ZG 之上。

MACD 已计算并写入数据，当前筛选逻辑未强制要求 MACD 条件；如需“MACD 在 0 轴上方”等，可在 `chan_logic.py` 或 `main.py` 中增加过滤。

## 网络断连 / RemoteDisconnected 说明

若出现 `Remote end closed connection without response`：

- **获取列表**：脚本会先尝试 `stock_info_a_code_name()`，失败再试 `stock_zh_a_spot_em()`，每种最多重试 3 次、间隔 3 秒。
- **逐只拉 K 线**：单次请求失败会重试 2 次（见 `data_fetcher.py`）；建议加 `--delay 1` 或更大，降低请求频率。
- 若仍频繁断连，可稍后再跑，或换网络/代理。
- 测试时建议：`python main.py --limit 20 --delay 1`。

## 文件说明

- `main.py`：入口，拉取股票列表、多级别 K 线、筛选、输出。
- `data_fetcher.py`：优先从库读 K 线；若数据不是最新则增量拉取并写入库，再返回库中数据；含 MACD 计算。
- `kline_db.py`：SQLite 读写（`data/stock_kline.db`），表结构 symbol + level + dt + OHLCV + MACD。
- `chan_logic.py`：缠论中枢识别与第三买点判断（简化实现）。
- `requirements.txt`：Python 依赖列表。
- `data/`：运行时生成，存放 `stock_kline.db`。
