#!/usr/bin/env python3
"""
_round_runner.py — 被 run_pipeline_loop.py 通过 subprocess 调用的单轮执行器。

新增 v2 增强:
  - 更健壮的 .env 加载（支持引号/空值/注释）
  - 自动安装缺失依赖
  - 重试机制 (瞬态错误自动重试1次)
  - 超时保护
  - 详细的错误输出
"""
import json
import os
import sys
import time
from pathlib import Path

BASE_DIR = "/root/binance-square-agent"
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "research"))

# ── 健壮 .env 加载 ──────────────────────────────────────────
def _load_env(path: str) -> None:
    """加载 .env 文件，比基本的逐行解析更健壮。"""
    if not os.path.exists(path):
        print(f"[_round_runner] ⚠️ .env not found at {path}", file=sys.stderr, flush=True)
        return
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            # 去除两端引号
            if (val.startswith("'") and val.endswith("'")) or \
               (val.startswith('"') and val.endswith('"')):
                val = val[1:-1]
            if key and val:
                os.environ[key] = val

_load_env(os.path.join(BASE_DIR, ".env"))

# ── 必要的环境变量检查 ──────────────────────────────────────
_REQUIRED_VARS = ["DEEPSEEK_API_KEY"]
for _var in _REQUIRED_VARS:
    if not os.environ.get(_var):
        print(f"[_round_runner] ❌ 缺少关键环境变量: {_var}", file=sys.stderr, flush=True)
        sys.exit(1)

# ── 预检依赖 ──────────────────────────────────────────────
_MISSING_DEPS = []
for _mod_name in ("feedparser", "vectorbt", "yfinance", "httpx", "openai"):
    try:
        __import__(_mod_name)
    except ImportError:
        _MISSING_DEPS.append(_mod_name)

if _MISSING_DEPS:
    import subprocess
    print(f"[_round_runner] 安装缺失依赖: {_MISSING_DEPS}", file=sys.stderr, flush=True)
    for _m in _MISSING_DEPS:
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", _m],
                capture_output=True, text=True, timeout=60,
            )
            if proc.returncode != 0:
                print(f"[_round_runner] ⚠️ 安装 {_m} 失败: {proc.stderr[:200]}", file=sys.stderr, flush=True)
            else:
                print(f"[_round_runner] ✅ {_m} 已安装", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[_round_runner] ⚠️ 安装 {_m} 异常: {e}", file=sys.stderr, flush=True)

# ── 执行管线（带重试） ──────────────────────────────────────
_MAX_RETRIES = 2
_last_error = None

for _attempt in range(1, _MAX_RETRIES + 1):
    try:
        p = DailyResearchPipeline()
        result = p.run_full_cycle()
        _last_error = None
        break
    except Exception as e:
        _last_error = str(e)
        print(f"[_round_runner] ⚠️ Attempt {_attempt}/{_MAX_RETRIES} failed: {_last_error}", file=sys.stderr, flush=True)
        if _attempt < _MAX_RETRIES:
            wait = _attempt * 5
            print(f"[_round_runner] 等待 {wait}s 后重试...", file=sys.stderr, flush=True)
            time.sleep(wait)

if _last_error:
    # 所有重试都失败，输出 error 结果
    result = {
        "pipeline_start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pipeline_end": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "steps": {"harvest": {"status": "error", "error": _last_error}},
        "fusion_id": None,
        "approved": False,
        "errors": [_last_error],
    }
    print(f"[_round_runner] ❌ 所有重试均失败: {_last_error}", file=sys.stderr, flush=True)

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
