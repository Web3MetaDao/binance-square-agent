#!/usr/bin/env python3
"""
_round_runner.py — 被 run_pipeline_loop.py 通过 subprocess 调用的单轮执行器。

同时添加 BASE_DIR/research 到 sys.path，确保 harvester.py 的 fallback
绝对导入（from sources import ...）能正确找到 research/sources 包。
"""
import json
import os
import sys
from pathlib import Path

BASE_DIR = "/root/binance-square-agent"
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "research"))  # 确保 sources 包可找到

# 手动加载 .env
env_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'").strip('"')
            if key:
                os.environ[key] = val

# ── 预检依赖 ──────────────────────────────────────────────
# 检查并修复缺失的依赖，防止相对导入链中途失败
_MISSING_DEPS = []
for _mod_name in ("feedparser", "vectorbt", "yfinance"):
    try:
        __import__(_mod_name)
    except ImportError:
        _MISSING_DEPS.append(_mod_name)

if _MISSING_DEPS:
    # 尝试用 pip 安装
    import subprocess
    print(f"[_round_runner] 安装缺失依赖: {_MISSING_DEPS}", file=sys.stderr, flush=True)
    for _m in _MISSING_DEPS:
        proc = subprocess.run(
            [sys.executable, "-m", "pip", "install", _m],
            capture_output=True, text=True, timeout=30,
        )
        if proc.returncode != 0:
            print(f"[_round_runner] ⚠️ 安装 {_m} 失败: {proc.stderr[:200]}", file=sys.stderr, flush=True)
        else:
            print(f"[_round_runner] ✅ {_m} 已安装", file=sys.stderr, flush=True)

# ── 导入管线 ──────────────────────────────────────────────
from research.pipeline import DailyResearchPipeline

p = DailyResearchPipeline()
result = p.run_full_cycle()

# 写入结果文件
round_num_str = os.environ.get("PIPELINE_ROUND_NUM", "unknown")
result_path = f"{BASE_DIR}/data/pipeline_result_round{round_num_str}.json"
Path(result_path).parent.mkdir(parents=True, exist_ok=True)
with open(result_path, "w") as f:
    json.dump(result, f, ensure_ascii=False, indent=2, default=str)

# 成功标记
with open(f"{BASE_DIR}/data/.round{round_num_str}_ok", "w") as f:
    f.write("OK")

# 输出结果 JSON 到 stdout（供父进程读取）
_sys_stdout = sys.stdout
_sys_stdout.write("__RESULT_JSON__\n")
_sys_stdout.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
_sys_stdout.flush()
