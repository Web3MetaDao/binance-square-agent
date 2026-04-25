#!/usr/bin/env python3
"""
hermes_w2e_script.py
====================
Hermes cron script: 每次触发时执行一次 W2E 发帖流程。
Hermes 会将此脚本的 stdout 注入到 Agent prompt 中作为上下文。
"""
import os
import sys
import datetime

# 加载 .env
env_file = '/root/binance-square-agent/.env'
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

os.chdir('/root/binance-square-agent')
sys.path.insert(0, '/root/binance-square-agent')


def main() -> int:
    try:
        from w2e_post_generator import W2EPostGenerator

        gen = W2EPostGenerator()
        result = gen.run_once()

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if result.get('success'):
            post_text = result.get('post_text', '')[:100]
            print(f"[{now}] W2E 发帖成功")
            print(f"参考博主: {result.get('source_creator', 'N/A')}")
            print(f"发帖内容预览: {post_text}...")
            print("状态: SUCCESS")
            return 0

        error = result.get('error') or result.get('reason') or '未知错误'
        print(f"[{now}] W2E 发帖失败: {error}")
        print("状态: FAILED")
        return 1
    except Exception as e:
        print(f"脚本执行异常: {e}")
        print("状态: ERROR")
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
