# 开发文档 — Binance Square AI Quant Trading Agent

> Version 2.1 | Last updated: 2026-05-02

---

## 📋 目录

1. [项目概览](#1-项目概览)
2. [架构总览](#2-架构总览)
3. [核心模块详解](#3-核心模块详解)
4. [数据流](#4-数据流)
5. [定时任务体系](#5-定时任务体系)
6. [开发规范](#6-开发规范)
7. [常见问题](#7-常见问题)

---

## 1. 项目概览

### 1.1 定位
全自动加密货币量化交易系统。从行情感知→信号检测→策略评分→风控→执行的全闭环管
线。同时具备自主研究管线（arXiv/GitHub/blog策略挖掘）。

### 1.2 核心指标
- **目标**: 100U → 2000U (20倍)
- **平台**: OKX (SWAP合约)
- **交易所**: OKX + Gate + Bitget (三源数据)
- **最大持仓**: 5个并行，每仓10-15%权益
- **止损**: 单笔-15%，总回撤-15%熔断

---

## 2. 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                    感知层 (Perception)                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────┐  │
│  │ 行情数据  │ │ 三所数据  │ │ 链上资金  │ │ 四维舆情      │  │
│  │ OKX API  │ │ Gate/BG  │ │ 大户追踪  │ │ OKX+Tw+Square│  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬────────┘  │
└───────┼─────────────┼────────────┼────────────────┼─────────┘
        │             │            │                │
┌───────▼─────────────▼────────────▼────────────────▼─────────┐
│                    检测层 (Detection)                        │
│  ┌──────────────────┐ ┌──────────────────┐                   │
│  │ Surge Scanner v2 │ │ Breakout Detector│                   │
│  │ 暴涨暴跌信号     │ │ 起涨点检测       │                   │
│  └────────┬─────────┘ └────────┬─────────┘                   │
│  ┌──────────────────┐ ┌──────────────────┐                   │
│  │ 庄家收筹雷达      │ │ OI 异动检测      │                   │
│  │ (Accumulation)   │ │ (Open Interest)  │                   │
│  └────────┬─────────┘ └────────┬─────────┘                   │
└───────────┼────────────────────┼─────────────────────────────┘
            │                    │
┌───────────▼────────────────────▼─────────────────────────────┐
│                    评分/决策层 (Scoring & Strategy)            │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐  │
│  │ scoring.py   │ │ signal_fusion │ │ strategy_engine     │  │
│  │ 22维度评分   │ │ 信号融合排序  │ │ 策略执行+建仓       │  │
│  └──────────────┘ └──────────────┘ └──────────┬───────────┘  │
└────────────────────────────────────────────────┼──────────────┘
                                                 │
┌────────────────────────────────────────────────▼──────────────┐
│                    风控/执行层 (Risk & Execution)              │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐  │
│  │ Risk Manager │ │ Position Mgr │ │ OKX Wrapper          │  │
│  │ 熔断/仓位控制 │ │ 持仓管理     │ │ 下单API封装          │  │
│  └──────────────┘ └──────────────┘ └──────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### 2.1 目录结构

```
binance-square-agent/
├── main.py                        # 主入口 (发帖/W2E/聪明钱/直播模式)
├── config/
│   ├── settings.py                # 全局配置 (密钥/权重/参数)
│   └── __init__.py
├── core/
│   ├── orchestrator.py            # 主流程编排
│   ├── state.py                   # 状态管理
│   ├── capabilities.py            # 能力注册
│   └── safety.py                  # 安全检查
├── layers/
│   ├── perception.py              # 感知层 - 行情扫描/趋势分析
│   ├── content.py                 # 内容生成层
│   ├── executor.py                # 执行层
│   └── builder.py                 # Persona构建器
├── exchange_fetchers/             # 行情数据采集
│   ├── okx_fetcher.py             # OKX 行情
│   ├── gate_fetcher.py            # Gate 行情
│   ├── bitget_fetcher.py          # Bitget 行情
│   ├── scoring.py                 # 22维度评分引擎
│   ├── kline_db.py                # K线SQLite缓存
│   ├── liquidation_detector.py    # 爆仓检测
│   └── large_taker_detector.py    # 大额吃单检测
├── okx_auto_trader/               # 自动交易系统 (100U→2000U)
│   ├── main.py                    # 入口 (scan/cycle)
│   ├── okx_wrapper.py             # OKX API 封装 (子进程调用CLI)
│   ├── breakout_rapid.py          # 起涨点检测 v1.6 双通道
│   ├── sentiment_scanner.py       # 四维舆情融合扫描器
│   ├── signal_fusion.py           # 多信号融合排序
│   ├── strategy_engine.py         # 策略引擎 (6种策略)
│   └── risk_manager.py            # 风控管理 (仓位/熔断)
├── smart_money/                   # 聪明钱追踪
│   ├── smart_money_monitor.py
│   ├── address_updater.py
│   ├── leaderboard_auto_update.py
│   ├── signal_to_content.py
│   └── telegram_scanner.py
├── accumulation_radar/            # 庄家收筹雷达
│   ├── scanner.py                 # OI+价格异动扫描
│   ├── strategy.py                # 评分策略
│   ├── report.py                  # 报告生成
│   └── swing.py                   # 摆盘检测
├── research/                      # 自主研究管线
│   ├── harvester.py               # 统一采集调度 (GitHub+arXiv+Blog)
│   ├── pipeline.py                # 管线编排 (解析→融合→审查→回测→部署)
│   ├── _round_runner.py           # 轮次执行器
│   └── sources/                   # 采集源实现
│       ├── github_quant.py
│       ├── arxiv_feed.py
│       └── blog_feed.py
├── backtest/                      # 回测系统
│   ├── engine.py                  # 回测引擎 (5级测试)
│   ├── pressure_test.py           # 压力测试
│   ├── monte_carlo.py             # 蒙特卡洛模拟
│   └── deploy_gate.py             # 部署审批
├── square_sentiment.py            # 币安广场W2E舆情第四维
├── breakout_detector.py           # 独立起涨点检测器
├── surge_scanner_v2.py            # 暴涨暴跌扫描 v2
├── hermes_accumulation_radar.py   # 庄家收筹雷达 (老版入口)
├── auto_quant_orchestrator.py     # 自主量化学习闭环编排器
├── auto_review.py                 # 自动复检
└── daily_review.py                # 每日复盘
```

---

## 3. 核心模块详解

### 3.1 四维舆情系统 (`sentiment_scanner.py`)

**维度权重** (全部可用时):

| 维度 | 权重 | 来源 | 更新频率 |
|------|------|------|----------|
| OKX 舆情 | 35% | OKX CLI sentiment-rank | 实时 |
| Twitter 热度 | 30% | OKX sentiment xMentionCnt | 实时 |
| 广场文章热度 | 20% | Square 文章公开API | 每30min |
| W2E创作者热搜 | 15% | W2E创作者帖文挖掘 | 每30min |

**动态降级**: 某维度不可用时权重自动重分配到其他维度，保持归一化。

**W2E第四维** (`square_sentiment.py`):
- 从W2E排行榜开始，每轮从帖子中提取新创作者
- 并行并发提帖 (8 workers)，提取 `$cashtag` + 文本匹配
- 情感分类 (bullish/neutral/bearish)
- 30分钟窗口统计提及频率

### 3.2 起涨点检测 (`breakout_rapid.py` v1.6)

**双通道结构**:
- **通道A (H级验证)**: 四级严格验证 (H1→H2→H3→H4)，零误报
- **通道B**: 捕获低价/低量起爆币 (自适应量级阈值)

**四级验证**:
```
H1: 价格突破前高 + 成交量放大
H2: 量级放大确认 (HIGH≥2M/MED≥500K/LOW≥50K USDT)
H3: 多时间框共振 (15min/1H/2H)
H4: 最终执行确认
```

**自适应量级**: HIGH≥2M / MED≥500K / LOW≥50K (USDT)，LOW通道阈1.2x/容差0.3%

### 3.3 评分引擎 (`exchange_fetchers/scoring.py`)

22维度评分体系，MAX_SCORE=151:

**核心维度** (P0): Supertrend(4) + ADX + BB+RSI + CandleWick + FVG + UMACD

**优化规则**:
- ADX<20 → 趋势分减半
- OFI+BB超卖共振 → +3
- Bonus扩至6分
- MA88>12% → 不计分 (起爆前远离均线)
- Chandelier Exit 独立权重4分

### 3.4 策略引擎 (`strategy_engine.py`)

6种策略类型:
1. **Breakout Follow** — 突破跟随
2. **Smart Money Follow** — 聪明钱跟随
3. **OI Anomaly** — 持仓量异动
4. **Oversold Reversal** — 超卖反转
5. **Sentiment Surge** — 舆情爆发
6. **Multi-Confirmation** — 多信号共振

### 3.5 风控系统 (`risk_manager.py`)

**三层防护**:
- **仓位层**: 单笔10-15%权益，最多5并行
- **风险层**: MCAP $10M-$500M过滤，OKX只做SWAP
- **熔断层**: 总回撤-15%熔断所有交易

### 3.6 自主研究管线 (`research/`)

**7x24闭环**: 采集→解析→融合→审查→回测→部署→注入
- **采集源**: GitHub(freqtrade/awesome-quant/qstrader)、arXiv、Blog RSS
- **解析**: LLM (DeepSeek/OpenAI) 结构化抽取策略逻辑
- **融合**: 与现有surge_scanner_v2策略融合
- **审查**: OverfitReviewer过拟合审查
- **回测**: 5级 (insample→outsample→pressure→slippage→monte carlo)
- **部署**: DeployGate审批后自动注入scoring.py权重

### 3.7 庄家收筹雷达 (`accumulation_radar/`)

- **OI扫描**: 实时监测持仓量异常增长
- **价格异动**: 检测吸筹/派发模式
- **摆盘检测**: 识别主力压单/托盘行为
- **三重评分**: 基础分+趋势分+信号分
- **Heat Tracker**: OI累计变化热力图

---

## 4. 数据流

### 4.1 信号→执行管线

```
OKX Market Filter ──┐
OI Scan ────────────┤
Smart Money ────────┤──→ 信号融合 ──→ 策略引擎 ──→ 风险检查 ──→ 建仓
四维舆情 ────────────┘               │                           │
Breakout ──────────┘                │                           │
                                     ▼                           ▼
                              score > 阈值                    OKX Wrapper
                              (动态调整)                      下单执行
```

### 4.2 数据持久化

```
data/
├── kline_cache.db           # K线SQLite缓存
├── mcap_cache.json          # 市值缓存
├── cg_mcap_cache.json       # CoinGecko市值缓存
├── square_hot_topics.json   # W2E热搜币种 (每30min更新)
├── square_creators_pool.json # 创作者池 (持续扩展)
├── breakout_dedup.json      # 起涨点去重
├── heat_history.json        # OI热力历史
└── review_db.json           # 复盘数据库
```

### 4.3 外部依赖

| 依赖 | 用途 | 认证方式 |
|------|------|----------|
| OKX CLI v1.3.2 | 行情/交易 | API Key + Secret |
| OpenAI API | LLM解析/内容生成 | API Key |
| Square API | W2E舆情采集 (read-only) | API Key |
| GitHub API | 研究管线采集 | PAT (public repos) |
| CoinGecko API | 市值数据 | 免费 (无需key) |
| Telegram Bot | 消息/信号推送 | Bot Token |

---

## 5. 定时任务体系

全部通过内置 cron scheduler 管理。

### 5.1 交易管道

| 任务 | 频率 | 功能 |
|------|------|------|
| okx-demo-scan | 每1h | 四维融合 + 起涨点检测 |
| 起涨点检测 | 每15min | breakout_detector.py |
| 策略信号扫描 | 每15min | 暴涨暴跌信号 (三所三时间框) |

### 5.2 舆情管道

| 任务 | 频率 | 功能 |
|------|------|------|
| W2E舆情扫描 | 每30min | square_sentiment.py 采集 |
| 策略→广场转发 | 每15min | 信号二次推送 |

### 5.3 监控管道

| 任务 | 频率 | 功能 |
|------|------|------|
| 抓庄雷达-OI评分 | 每1h | accumulation_radar OI扫描 |
| 抓庄雷达-异动评分 | 每1h | 价格/交易异动 |
| 三源合约冲榜预警 | 每1h | tri_surge_alarm.py |
| 量化自主复检 | 每4h | auto_review.py --quick |
| 每日复盘 | 08/20 UTC | daily_review.py |

### 5.4 研究管道

| 任务 | 频率 | 功能 |
|------|------|------|
| 研究管线汇报 | 每4h | pipeline 状态汇报 |
| 量化学习-早班 | 07 UTC | arXiv搜索 |
| 量化学习-午班 | 13 UTC | GitHub挖掘 |
| 量化学习-晚班 | 18 UTC | 消化注入 |
| 量化闭环编排器 | 08/20 UTC | 全流程编排 |
| 庄家收筹-pool | 10 UTC | 每日池扫描 |

---

## 6. 开发规范

### 6.1 代码质量要求
- 所有模块必须通过 `python3 -c "import ast; ast.parse(open(file).read())"` 语法检查
- 企业级代码审计并行开发流程 (详见 `enterprise-code-audit` skill)
- SQLite 多进程场景必须 `PRAGMA foreign_keys=ON` + 显式 `c.commit()` + 写锁

### 6.2 新增模块规范
1. 先写 `__main__` 入口支持 `once` 和 `cron` 模式
2. 日志使用 `logging`，统一格式
3. 敏感信息不硬编码，从 `config.settings` 或 `.env` 读取
4. 运行时数据写入 `data/` 目录，不污染项目根

### 6.3 API 密钥管理
```
.env 文件格式（已 gitignore，绝不提交）:
OPENAI_API_KEY=sk-xxx
SQUARE_API_KEY=xxx
TG_BOT_TOKEN=xxx
SIGNAL_BOT_TOKEN=xxx
COINALYZE_API_KEY=xxx
```

### 6.4 测试
```
python3 test_*.py          # 单元测试
python3 test_integration.py # 集成测试
python3 -m pytest          # (如配置) pytest 测试
```

---

## 7. 常见问题

### Q: DeepSeek API key 失效怎么办？
研究管线的 `paper_parser.py` 调用 DeepSeek 解析策略。如 key 失效:
1. 在 `.env` 设置有效的 `DEEPSEEK_API_KEY`
2. 或在 `research/parsers/paper_parser.py` 中将 `LLM_BASE_URL` 改为可用的 OpenAI 兼容 API

### Q: Square API 返回 404？
从 v2.1 起广场已将 `/public/pgc/` 端点废弃。当前有效路径:
- `/friendly/gateway` 基础路径
- `user/client` (用户信息)
- `queryUserProfilePageContentsWithFilter` (帖子内容)
- W2E 排行榜 (无需认证)

### Q: Kline 缓存如何处理？
`kline_db.py` 使用 SQLite 缓存 K 线数据。缓存过期自动刷新。
如遇到 `database is locked` 错误:
- 检查 `with_lock` 上下文使用
- 确认 `isolation_level=None` + 显式 commit

### Q: 新增策略信号如何注入？
1. 实现信号检测函数，返回统一的信号 dict 格式
2. 在 `signal_fusion.py` 的融合管线注册
3. 在 `scoring.py` 添加对应维度权重
4. 在 `breakout_rapid.py` 验证通道中添加检查

---

> 本文档由 Hermes Agent 自动维护 | 如有更新建议请提交 PR
