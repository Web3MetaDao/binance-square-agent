# 币安广场运营系统智能体
> 基于 ClawSelf 架构的全自动加密货币内容运营系统，内置币安广场**内容挖矿（Write to Earn）**优化策略

## 内容挖矿说明

本智能体已针对币安广场 [Write to Earn](https://www.binance.com/zh-CN/academy/articles/write-to-earn-on-binance-square-all-you-need-to-know) 计划进行深度优化：

- 每条帖子自动植入 **cashtag**（如 `$BTC`、`$ETH`），这是触发挖矿返佣的关键
- 读者点击帖子中的 cashtag 后完成交易，创作者即可获得手续费分成
- 基础返佣 **20%**，周榜 Top30 最高 **50%**，每周四以 **USDC** 结算
- 帖子有效期 **7 天**，系统保持高频发帖以最大化挖矿收益

## 快速部署

### 1. 安装依赖

```bash
pip3 install openai requests
```

### 2. 配置环境变量

```bash
export SQUARE_API_KEY="your_binance_square_openapi_key"
export OPENAI_API_KEY="your_openai_api_key"
export OPENAI_BASE_URL="https://your-proxy.example.com/v1"  # 可选：第三方中转站
export LLM_MODEL="gpt-5.5"                                  # 可选：默认 gpt-5.5
```

### 3. 进入项目目录

```bash
cd /home/ubuntu/binance-square-agent
```

### 4. 启动灵魂提取（首次使用必做）

```bash
python3 main.py build-quick   # 快速版（约15分钟）
python3 main.py build         # 完整版（约60分钟）
```

### 5. 扫描热点（测试感知层）

```bash
python3 main.py scan
```

### 6. 启动全自动发帖

```bash
python3 main.py start
```

## 系统架构

```
binance-square-agent/
├── main.py                  # 主程序入口
├── config/
│   └── settings.py          # 所有可配置参数
├── core/
│   ├── state.py             # 状态管理（持久化）
│   └── orchestrator.py      # 总控编排器
├── layers/
│   ├── builder.py           # 灵魂提取层（100问访谈）
│   ├── perception.py        # 感知层（双端热点扫描）
│   ├── content.py           # 内容层（LLM短贴生成 + 内容挖矿优化）
│   └── executor.py          # 执行层（发帖+配额控制）
├── smart_money/             # 聪明钱监控模块
├── live/                    # 数字人直播模块
├── data/
│   ├── persona.md           # 用户人设文件（访谈后生成）
│   ├── market_context.json  # 最新市场热点数据
│   └── agent_state.json     # 智能体运行状态
└── logs/
    └── post_log.jsonl       # 发帖日志（JSONL格式）
```

## 核心参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| DAILY_LIMIT | 100 | 每日最大发帖数 |
| MIN_INTERVAL_MIN | 14 | 两贴之间最短间隔（分钟） |
| COIN_COOLDOWN_H | 4 | 同币种最短间隔（小时） |
| SCAN_INTERVAL_M | 30 | 感知层扫描间隔（分钟） |
| LLM_MODEL | gpt-5.5 | 使用的LLM模型（支持环境变量覆盖） |
| OPENAI_BASE_URL | https://api.openai.com/v1 | OpenAI 接口地址（支持第三方中转站） |

## 发帖错误码

| 错误码 | 说明 | 处理方式 |
|--------|------|----------|
| 000000 | 成功 | 记录日志 |
| 220003 | API Key 无效 | 检查 SQUARE_API_KEY |
| 20022 | 内容含敏感词 | 重新生成内容 |
| 2000001 | 账号被封禁 | 触发熔断，停止发帖 |
