# 部署文档 — Binance Square AI Quant Trading Agent

> Version 2.1 | Last updated: 2026-05-02

---

## 📋 目录

1. [环境要求](#1-环境要求)
2. [快速部署](#2-快速部署)
3. [配置说明](#3-配置说明)
4. [启动指南](#4-启动指南)
5. [运维操作](#5-运维操作)
6. [迁移指南](#6-迁移指南)
7. [故障排查](#7-故障排查)
8. [备份与恢复](#8-备份与恢复)

---

## 1. 环境要求

### 1.1 硬件要求
| 组件 | 最低要求 | 推荐 |
|------|---------|------|
| CPU | 2核 | 4核+ |
| 内存 | 2GB | 4GB+ |
| 磁盘 | 20GB | 40GB+ |
| 网络 | 公网IP (可访问OKX/GitHub) | 同左 |

### 1.2 软件要求
| 组件 | 版本 | 说明 |
|------|------|------|
| Linux | Ubuntu 22.04+ / Debian 12+ | 推荐 Ubuntu |
| Python | 3.10+ | 3.11 最优 |
| Git | 2.30+ | 代码仓库管理 |
| OKX CLI | 1.3.2 | 行情/交易 CLI |
| Go | 1.21+ | OKX CLI 依赖 |

### 1.3 所需API密钥
| 密钥 | 用途 | 获取方式 |
|------|------|----------|
| OKX API Key | 行情+交易 | OKX 开发者中心 |
| OpenAI API Key | LLM 解析/内容生成 | platform.openai.com |
| Square API Key | 币安舆情采集 | 币安广场开发者 |
| Telegram Bot Token | 消息推送 | @BotFather |
| Coinalyze API Key | 多所OI数据 | coinalyze.net |

---

## 2. 快速部署

### 2.1 首次部署

```bash
# 1. 克隆仓库
git clone https://github.com/Web3MetaDao/binance-square-agent.git
cd binance-square-agent

# 2. 安装 Python 依赖
pip install -r requirements.txt

# 3. 安装 OKX CLI
# 参考: https://www.okx.com/zh-cn/help/how-to-install-okx-cli
# 确认版本:
okx --version  # 应 >= 1.3.2

# 4. 配置密钥
cp .env.example .env   # 如不存在示例，手动创建
```

### 2.2 密钥配置

编辑 `.env` 文件：

```bash
# === OpenAI (LLM 核心) ===
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OPENAI_BASE_URL=https://api.openai.com/v1
LLM_MODEL=gpt-5.5

# === 币安广场舆情 (read-only) ===
SQUARE_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# === Telegram 推送 ===
TG_BOT_TOKEN=xxxxxxxxxx:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TG_CHAT_ID=1077054086
SIGNAL_BOT_TOKEN=xxxxxxxxxx:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# === OKX 交易 ===
OKX_API_KEY=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
OKX_SECRET_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OKX_PASSPHRASE=xxxxxxxxxx
OKX_PROFILE=hermes-trader

# === Coinalyze (多所 OI) ===
COINALYZE_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

> ⚠️ **安全警告**: `.env` 文件已在 `.gitignore` 中，不会被提交。切勿改名或复制为不含 `.gitignore` 保护的名称。

### 2.3 验证环境

```bash
# 检查 Python 和依赖
python3 -c "import requests, numpy, pandas; print('Python OK')"

# 检查 OKX CLI
okx account balance --json | python3 -m json.tool

# 测试 Square API
curl -s "https://www.binance.com/bapi/square/v1/friendly/gateway" | head -c 200

# 测试 LLM API
python3 -c "
import os
from openai import OpenAI
client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
r = client.chat.completions.create(model='gpt-4o', messages=[{'role':'user','content':'hi'}])
print(f'LLM OK: {r.choices[0].message.content[:20]}')
"
```

---

## 3. 配置说明

### 3.1 核心配置 (`config/settings.py`)

```python
# 关键参数说明 (settings.py):
LLM_MODEL = "gpt-5.5"           # 内容生成模型
SQUARE_CONTENT_COOLDOWN = 900   # 发帖冷却 (秒)
MAX_POSTS_PER_CYCLE = 4         # 单轮最大发帖数
PERSONA_DIR = "layers/personas" # 人设模板目录
```

### 3.2 评分权重 (`exchange_fetchers/scoring.py`)

```
WEIGHTS = {
    "supertrend": 4,    # 趋势强度
    "adx": 3,           # 趋势确认
    "bb_rsi": 3,        # 布林带+RSI
    "candle_wick": 3,   # 影线信号
    "fvg": 3,           # 公允价值缺口
    "umacd": 3,         # UMACD动量
    "chandelier": 4,    # Chandelier Exit (独立权重)
    # ... 共22维度
}
MAX_SCORE = 151
```

### 3.3 四维舆情权重 (`okx_auto_trader/sentiment_scanner.py`)

```python
OKX_WEIGHT = 35          # OKX舆情
TWITTER_WEIGHT = 30      # 推特舆情
SQUARE_WEIGHT = 20       # 币安广场文章热度
SQUARE_W2E_WEIGHT = 15   # W2E创作者热搜 (第四维)
```

> 某维度数据不可用时，权重自动归一化分配到其他维度。

### 3.4 风控参数 (`okx_auto_trader/risk_manager.py`)

| 参数 | 默认值 | 说明 |
|------|--------|------|
| MAX_CONCURRENT_POSITIONS | 5 | 最大并行持仓 |
| POSITION_SIZE_PCT | 0.10-0.15 | 单仓权益占比 |
| MAX_DRAWDOWN_PCT | -0.15 | 总回撤熔断阈值 |
| MCAP_MIN | 10_000_000 | 最低市值 ($10M) |
| MCAP_MAX | 500_000_000 | 最高市值 ($500M) |

---

## 4. 启动指南

### 4.1 一键启动 (所有管道)

```bash
# 启动常驻后台（发帖/监控/聪明钱扫描）
nohup python3 main.py start > logs/main.log 2>&1 &

# 查看状态
python3 main.py status
python3 main.py status-json
```

### 4.2 单独启动各模块

```bash
# 四维融合扫描 (交易触发)
cd okx_auto_trader && python3 main.py scan

# 全周期执行 (扫描→决策→建仓)
cd okx_auto_trader && python3 main.py cycle

# W2E舆情采集
python3 square_sentiment.py once

# 四维舆情扫描
cd okx_auto_trader && python3 sentiment_scanner.py

# 暴涨暴跌信号
python3 surge_scanner_v2.py

# 起涨点检测
python3 breakout_detector.py

# 庄家收筹
python3 run_radar.py

# 每日复盘
python3 daily_review.py

# 研究管线 (单轮)
cd research && python3 run_pipeline_loop.py --once

# 量化自检
python3 auto_review.py --quick
```

### 4.3 定时任务管理

使用内置 cron scheduler：

```bash
# 查看所有定时任务
cronjob list

# 手动触发任务
cronjob run --job-id <job_id>

# 任务状态一览
# (通过 cronjob list 查看 last_status / next_run_at)
```

建议的部署方案（一次性执行所有 cronjob 注册，已在生产服务器配置）:

| 任务 | 类型 | 首次部署需手动注册 |
|------|------|-------------------|
| W2E舆情 | ⏰ 30min | ✅ 已注册 |
| 四维融合扫描 | ⏰ 1h | ✅ 已注册 |
| 起涨点检测 | ⏰ 15min | ✅ 已注册 |
| 抓庄雷达 | ⏰ 1h | ✅ 已注册 |
| 三源预警 | ⏰ 1h | ✅ 已注册 |
| 量化复检 | ⏰ 4h | ✅ 已注册 |
| 每日复盘 | ⏰ 08/20 UTC | ✅ 已注册 |

> **迁移后需重新注册定时任务**：迁移后 cron scheduler 的 `jobs.json` 会丢失状态。
> 使用 `cronjob` 管理工具在工作会话中重新创建。

---

## 5. 运维操作

### 5.1 日常检查

```bash
# 1. 检查所有Python进程
ps aux | grep python3 | grep -v grep

# 2. 检查系统资源
free -h                    # 内存
df -h /                    # 磁盘 (确保 >10% 剩余)
uptime                     # CPU负载 (应 < 核心数)

# 3. 检查最新数据
ls -lt data/*.json | head -10

# 4. 检查日志
tail -50 logs/main.log

# 5. 检查定时任务状态
# (在会话中使用 cronjob list)
```

### 5.2 重启流程

```bash
# 停止所有
pkill -f "main.py start"        # 停止主进程
pkill -f "lanaai_daemon"        # 停止监控守护
pkill -f "pipeline_loop"        # 停止研究管线 (如需)
pkill -f "_round_runner"        # 停止轮次执行 (如需)

# 确认停止
ps aux | grep python3 | grep -v grep | wc -l

# 重新启动
nohup python3 main.py start > logs/main.log 2>&1 &
```

### 5.3 数据清理

```bash
# 清理过期的运行时数据 (保留最后一天)
find data/ -name "round*.json" -mtime +3 -delete
find data/ -name "pipeline_result*.json" -mtime +3 -delete

# 清理研究管线的中间产物
find data/ -name ".*_ok" -mtime +1 -delete
find data/ -name ".*_error" -mtime +1 -delete

# 清理 Kline 缓存 (如果误写/需要强制刷新)
rm data/kline_cache.db
```

### 5.4 拉取更新

```bash
# 保留本地 .env 和运行时数据
git stash
git pull
git stash pop  # 恢复本地修改

# 同步依赖
pip install -r requirements.txt --upgrade
```

---

## 6. 迁移指南

### 6.1 完整迁移步骤

```bash
# === 源服务器 ===

# 1. 推送最新代码到 GitHub (如未自动推送)
git add -A
git commit -m "pre-migration snapshot"
git push origin main

# 2. 记录当前 cron 任务列表
# (用 cronjob list 截图保存)

# 3. 备份 .env 文件
cat .env   # 或 scp 到目标服务器

# === 目标服务器 ===

# 4. 克隆代码
git clone https://github.com/Web3MetaDao/binance-square-agent.git
cd binance-square-agent

# 5. 安装依赖
pip install -r requirements.txt

# 6. 创建并配置 .env
# (从源服务器复制内容)

# 7. 验证 OKX CLI
okx account balance --json

# 8. 验证 LLM API
python3 -c "
import os; from openai import OpenAI
c = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
print(c.chat.completions.create(model='gpt-4o', messages=[{'role':'user','content':'ping'}]).choices[0].message.content)
"

# 9. 启动服务
nohup python3 main.py start > logs/main.log 2>&1 &

# 10. 在 Hermes Agent 会话中重新注册 cron 任务
# (参考 5. 定时任务体系)
```

### 6.2 备份文件清单

迁移时需要手动转移的敏感文件:

| 文件 | 位置 | 说明 |
|------|------|------|
| `.env` | 项目根 | API密钥 (不提交) |
| `data/kline_cache.db` | 可选 | K线缓存可重新生成 |
| cron 状态 | cron/jobs.json | 定时任务注册 (需重新注册) |

---

## 7. 故障排查

### 7.1 常见错误

| 症状 | 原因 | 解决 |
|------|------|------|
| `401 Unauthorized` (API) | API key 过期/错误 | 检查 `.env` 中的 key |
| `ModuleNotFoundError` | 依赖未安装 | `pip install -r requirements.txt` |
| `database is locked` | SQLite 多进程冲突 | 检查 `with_lock` 用法 |
| `Square API 404` | 端点已废弃 | 使用 `/friendly/gateway` 路径 |
| `CLI crash: ts not supported` | OKX CLI v1.3.2 bug | 智能钱命令会崩溃，回避 |
| 磁盘满 | 日志/缓存持续增长 | `find data/ -mtime +7 -delete` |

### 7.2 日志查看

```bash
# 主程序日志
tail -100 logs/main.log

# 交易系统日志
tail -100 okx_auto_trader/data/last_scan.json  # 最近扫描

# 舆情采集日志 (通过 stdout 查看)
cd okx_auto_trader && python3 sentiment_scanner.py 2>&1

# 研究管线日志
tail -50 research/pipeline_loop.log
```

### 7.3 恢复流程

如果系统进入紧急状态（例如回撤超限、API失效）:

```bash
# 1. 熔断: 停止所有交易程序
pkill -f "main.py"
pkill -f "breakout"
pkill -f "okx_wrapper"

# 2. 检查原因
tail -100 logs/main.log | grep -iE 'error|fail|exception'

# 3. 修复问题 (改配置/换key/恢复env)

# 4. 重新启动
nohup python3 main.py start > logs/main.log 2>&1 &

# 5. 验证: 先 scan (dry-run) 再 cycle
cd okx_auto_trader && python3 main.py scan
```

---

## 8. 备份与恢复

### 8.1 GitHub 备份仓库

| 仓库 | 内容 | 访问权限 |
|------|------|----------|
| `Web3MetaDao/binance-square-agent` | 量化交易全量代码 | 私有 |
| `Web3MetaDao/hermes-agent` | Hermes Agent 源码 (含skills) | 私有 |
| `Web3MetaDao/hermes-agent-config` | 技能+配置模板 (密钥已去除) | 私有 |

### 8.2 备份 CLI 配置（hermes-agent-config）

```bash
git clone https://github.com/Web3MetaDao/hermes-agent-config.git
cp hermes-agent-config/config.yaml ~/.hermes/config.yaml
# 然后填入真实 api_key
```

### 8.3 手动备份清单

```bash
# 创建完整备份包 (排除 venv/node_modules 等大目录)
tar czf backup-$(date +%Y%m%d).tar.gz \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='venv' \
  --exclude='node_modules' \
  --exclude='data/kline_cache.db' \
  --exclude='data/state.db' \
  /root/binance-square-agent
```

---

## 附录 A: 生产服务器快照 (2026-05-02)

| 项目 | 数值 |
|------|------|
| 内存 | 1.9G 总量 / 914M 已用 |
| CPU | 负载 0.07/0.31/0.44 (4核) |
| 磁盘 | 39G 总量 / 8.9G剩余 (23%) |
| Python进程 | 12 个 (含研究管线、常驻主进程) |
| 运行时间 | 11天 |
| API密钥配置 | OKX(hermes-trader), OpenAI, Square, TG |

## 附录 B: 快速命令速查

```bash
# === 启动 ===
nohup python3 main.py start > logs/main.log 2>&1 &

# === 状态 ===
python3 main.py status
ps aux | grep python3 | grep -v grep | wc -l

# === 交易扫描 (dry-run) ===
cd okx_auto_trader && python3 main.py scan

# === 舆情扫描 ===
cd okx_auto_trader && python3 sentiment_scanner.py
python3 square_sentiment.py once

# === 检测 ===
python3 breakout_detector.py        # 起涨点
python3 surge_scanner_v2.py         # 暴涨暴跌

# === 研究 ===
cd research && python3 run_pipeline_loop.py --once

# === 复检 ===
python3 auto_review.py --quick

# === 停服 ===
pkill -f "main.py start"
```
