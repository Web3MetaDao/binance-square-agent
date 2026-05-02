#!/usr/bin/env python3
"""
run_pipeline_loop.py — 策略研究管线 7×24 小时循环运行。

每次都执行 run_full_cycle()，跑完后立即下一轮（无间隔）。
持续运行 TOTAL_DAYS 天后自动退出。

优化说明（v2）:
  1. 子进程使用 subprocess + sys.executable，避免 multiprocessing fork 的环境差异
  2. 错误退避：连续失败后指数退避（1min → 2min → 4min … max 30min）
  3. 优雅退出信号处理（SIGTERM/SIGINT 时保存状态）
  4. 每轮结果保留最近20轮，删除旧文件避免磁盘膨胀
  5. 更详细的进度日志 + 健康检查
"""

import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 预检依赖 ──────────────────────────────────────────────
# 在启动前自动安装缺失的依赖
_MISSING_DEPS = []
for _mod_name in ("feedparser", "vectorbt", "yfinance"):
    try:
        __import__(_mod_name)
    except ImportError:
        _MISSING_DEPS.append(_mod_name)

if _MISSING_DEPS:
    import subprocess as _sp
    print(f"[init] 安装缺失依赖: {_MISSING_DEPS}")
    for _m in _MISSING_DEPS:
        _proc = _sp.run(
            [sys.executable, "-m", "pip", "install", _m],
            capture_output=True, text=True, timeout=30,
        )
        if _proc.returncode != 0:
            print(f"[init] ⚠️ 安装 {_m} 失败: {_proc.stderr[:200]}")
        else:
            print(f"[init] ✅ {_m} 已安装")
# ── 确保 research 模块可导入 ──────────────────────────────
BASE_DIR = "/root/binance-square-agent"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ── 配置 ─────────────────────────────────────────────────
TOTAL_DAYS = 30                 # 持续天数
STATE_FILE = f"{BASE_DIR}/data/pipeline_loop_state.json"
TIMEOUT_PER_ROUND = 1200        # 每轮最长执行时间（秒）
MAX_KEEP_RESULTS = 20           # 保留最近多少轮的结果文件
MAX_BACKOFF_MINUTES = 30        # 最大退避时间（分钟）

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pipeline_loop] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline_loop")


# ── 信号处理 ─────────────────────────────────────────────

_shutdown_requested = False

def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.warning(f"收到信号 {signum}，正在优雅关闭...")


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── 状态管理 ─────────────────────────────────────────────

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"rounds_done": 0, "started_at": datetime.now(timezone.utc).isoformat(),
                "consecutive_failures": 0, "last_round_at": None, "max_rounds_done": 0}


def save_state(state: dict):
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ── 轮次执行 ─────────────────────────────────────────────

def _run_round_via_subprocess(round_num: int, base_dir: str) -> dict:
    """通过 subprocess 执行一轮管线（用 .py 入口文件保证相对导入正常）"""
    env = {**os.environ, "PIPELINE_ROUND_NUM": str(round_num),
           "PYTHONUNBUFFERED": "1"}
    runner_path = os.path.join(base_dir, "research", "_round_runner.py")

    proc = subprocess.run(
        [sys.executable, runner_path],
        capture_output=True, text=True,
        timeout=TIMEOUT_PER_ROUND,
        env=env,
    )

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    # 从 stdout 中提取结果 JSON
    marker = "__RESULT_JSON__"
    if marker in stdout:
        json_part = stdout.split(marker, 1)[1].strip()
        result = json.loads(json_part)
        if stderr.strip():
            errors = result.get("errors", [])
            errors.append(f"stderr: {stderr.strip()[:500]}")
            result["errors"] = errors
        return result
    else:
        error_text = stdout[:2000] + "\n---STDERR---\n" + stderr[:2000]
        raise RuntimeError(f"子进程未返回结果JSON\n{error_text}")


def _cleanup_old_results(base_dir: str, round_num: int):
    """保留最近 MAX_KEEP_RESULTS 轮的结果文件"""
    prefix_patterns = [
        f"{base_dir}/data/pipeline_result_round",
        f"{base_dir}/data/.round",
    ]
    for prefix in prefix_patterns:
        files = []
        try:
            for f in os.listdir(f"{base_dir}/data/"):
                if f.startswith(os.path.basename(prefix)):
                    # 提取轮次号
                    suffix = f.replace(os.path.basename(prefix), "")
                    try:
                        r = int("".join(c for c in suffix if c.isdigit()))
                        files.append((r, f))
                    except ValueError:
                        pass
        except FileNotFoundError:
            continue

        files.sort(key=lambda x: x[0])
        # 删除旧的
        for r, fn in files[:-MAX_KEEP_RESULTS]:
            path = os.path.join(f"{base_dir}/data/", fn)
            try:
                os.remove(path)
            except OSError:
                pass


# ── 主循环 ───────────────────────────────────────────────

def main():
    logger.info("🚀 策略研究管线 7×24 循环启动 (v2)")
    logger.info(f"配置: 持续{TOTAL_DAYS}d, 每轮超时{TIMEOUT_PER_ROUND}s, "
                f"保留最近{MAX_KEEP_RESULTS}轮结果")

    state = load_state()
    started = datetime.fromisoformat(state["started_at"])
    rounds_done = state.get("rounds_done", 0)
    consecutive_failures = state.get("consecutive_failures", 0)
    max_rounds_done = state.get("max_rounds_done", 0)
    deadline = started + timedelta(days=TOTAL_DAYS)

    logger.info(f"启动时间: {started.isoformat()}")
    logger.info(f"截止时间: {deadline.isoformat()}")
    logger.info(f"已完成轮次: {rounds_done} (历史峰值: {max_rounds_done})")
    logger.info(f"连续失败次数: {consecutive_failures}")

    round_num = rounds_done + 1
    round_start_time = datetime.now(timezone.utc)

    while datetime.now(timezone.utc) < deadline and not _shutdown_requested:
        logger.info(f"━━━ 第 {round_num} 轮开始 ━━━")

        # ── 错误退避 ──
        if consecutive_failures > 2:
            backoff_minutes = min(
                MAX_BACKOFF_MINUTES,
                2 ** (consecutive_failures - 2)  # 1→2→4→8→16→30
            )
            logger.warning(f"连续失败 {consecutive_failures} 次，退避 {backoff_minutes} 分钟")
            wait_until = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)

            # 分段等待，可被信号中断
            while datetime.now(timezone.utc) < wait_until and not _shutdown_requested:
                remaining = int((wait_until - datetime.now(timezone.utc)).total_seconds())
                if remaining > 0:
                    time.sleep(min(5, remaining))

            if _shutdown_requested:
                break

        # ── 执行一轮（subprocess） ──
        round_ok = False
        error_msg = None
        try:
            result = _run_round_via_subprocess(round_num, BASE_DIR)

            # 写结果文件
            result_path = f"{BASE_DIR}/data/pipeline_result_round{round_num}.json"
            Path(result_path).parent.mkdir(parents=True, exist_ok=True)
            with open(result_path, "w") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)

            # 快速成功标记
            with open(f"{BASE_DIR}/data/.round{round_num}_ok", "w") as f:
                f.write("OK")

            # 解析结果
            steps = result.get("steps", {})
            approved = result.get("approved", False)
            errors = result.get("errors", [])
            fusion_id = result.get("fusion_id", "N/A")
            logger.info(f"✅ 第 {round_num} 轮完成")
            logger.info(f"  融合ID: {fusion_id}")
            logger.info(f"  审批: {'✅ 通过' if approved else '⏸ 未通过'}")
            logger.info(f"  步骤数: {len(steps)}")
            if errors:
                logger.warning(f"  ⚠️ 错误: {len(errors)}个")
                for e in errors[:3]:
                    logger.warning(f"    {str(e)[:200]}")

            # 打印摘要
            print(f"\n📊 研究管线第{round_num}轮报告")
            print(f"  状态: {'✅ 已通过' if approved else '⏸ 未通过'}")
            print(f"  融合ID: {fusion_id}")
            for k, v in steps.items():
                if isinstance(v, dict):
                    status = v.get("status", "?")
                    detail = f" ({v.get('reason', '')})" if v.get("reason") else ""
                    print(f"  {k}: {status}{detail}")
                else:
                    print(f"  {k}: {v}")
            if errors:
                print(f"  ⚠️ 错误: {len(errors)}个")

            # 清理旧结果
            _cleanup_old_results(BASE_DIR, round_num)

            consecutive_failures = 0
            round_ok = True

        except subprocess.TimeoutExpired:
            error_msg = f"超时 ({TIMEOUT_PER_ROUND}s)"
            logger.error(f"❌ 第 {round_num} 轮超时 (> {TIMEOUT_PER_ROUND}s)")
            print(f"\n⚠️ 研究管线第{round_num}轮超时")
            consecutive_failures += 1

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ 第 {round_num} 轮失败: {e}")
            print(f"\n⚠️ 研究管线第{round_num}轮失败: {e}")

            # 写错误文件
            import traceback
            err_path = f"{BASE_DIR}/data/.round{round_num}_error"
            with open(err_path, "w") as f:
                f.write(f"{type(e).__name__}: {e}\n{traceback.format_exc()}")

            consecutive_failures += 1

        # ── 更新状态 ──
        state["rounds_done"] = round_num
        state["max_rounds_done"] = max(max_rounds_done, round_num)
        state["consecutive_failures"] = consecutive_failures
        state["last_round_at"] = datetime.now(timezone.utc).isoformat()
        state["last_round_ok"] = round_ok
        state["last_error"] = error_msg
        save_state(state)

        round_num += 1

        # ── 判断是否继续 ──
        if datetime.now(timezone.utc) >= deadline:
            logger.info("🏁 已到达截止时间，退出")
            break

        if _shutdown_requested:
            logger.info("🛑 收到退出信号，停止循环")
            break

        # ── 连续执行：立即下一轮（除非出错退避） ──
        if round_ok:
            elapsed = datetime.now(timezone.utc) - round_start_time
            logger.info(f"▶️ 第 {round_num-1} 轮耗时 {elapsed}")
            logger.info(f"   已运行: {(datetime.now(timezone.utc) - started)}")
            logger.info(f"   截止时间: {deadline.isoformat()}")
            round_start_time = datetime.now(timezone.utc)
        # 退避情况下已在上面等待

    # ── 最终报告 ──
    total_rounds = state["rounds_done"]
    elapsed = datetime.now(timezone.utc) - started
    logger.info(f"🏁 策略研究管线 7×24 循环结束")
    logger.info(f"   总轮次: {total_rounds} (历史峰值: {max(max_rounds_done, total_rounds)})")
    logger.info(f"   运行时长: {elapsed}")
    logger.info(f"   退出原因: {'信号中断' if _shutdown_requested else '到达截止时间'}")
    print(f"\n🏁 策略研究管线 7×24 运行结束")
    print(f"   总轮次: {total_rounds}")
    print(f"   运行时长: {elapsed}")
    print(f"   退出原因: {'信号中断' if _shutdown_requested else '到达截止时间'}")


if __name__ == "__main__":
    main()
