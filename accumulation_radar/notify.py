"""TG 通知发送模块。

提供 send_telegram() 发送消息到 Telegram，
以及 send_test_message() 用于快速验证配置。
"""

import logging

import requests

from .config import TG_BOT_TOKEN, TG_CHAT_ID

logger = logging.getLogger("accumulation_radar")

# Telegram 单条消息最大字符数限制
_MAX_CHARS = 4096


def send_telegram(
    text: str,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> bool:
    """发送消息到 Telegram。

    Parameters
    ----------
    text : str
        消息内容。
    bot_token : str | None
        Bot Token，为空时从 config.TG_BOT_TOKEN 读取。
    chat_id : str | None
        聊天 ID，为空时从 config.TG_CHAT_ID 读取。

    Returns
    -------
    bool
        全部发送成功返回 True，否则返回 False。
    """
    token = bot_token or TG_BOT_TOKEN
    cid = chat_id or TG_CHAT_ID

    if not token or not cid:
        logger.warning(
            "[TG] 未配置 TG_BOT_TOKEN 或 TG_CHAT_ID，跳过发送"
        )
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    session = requests.Session()

    # ── 消息分片 ────────────────────────────────────────
    chunks = _split_text(text, _MAX_CHARS)

    if len(chunks) > 1:
        logger.info("[TG] 消息较长，分为 %d 片发送", len(chunks))

    all_ok = True
    for idx, chunk in enumerate(chunks):
        # 前 N-1 块追加续传标识
        payload = chunk
        if idx < len(chunks) - 1:
            payload = chunk.rstrip() + "\n\n(continued...)"

        try:
            resp = session.post(
                url,
                data={
                    "chat_id": cid,
                    "text": payload,
                    "parse_mode": "HTML",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                logger.info(
                    "[TG] 第 %d/%d 片发送成功 (%d chars)",
                    idx + 1,
                    len(chunks),
                    len(payload),
                )
            else:
                logger.error(
                    "[TG] 第 %d/%d 片发送失败: HTTP %d, %s",
                    idx + 1,
                    len(chunks),
                    resp.status_code,
                    resp.text[:200],
                )
                all_ok = False
        except requests.RequestException as e:
            logger.error(
                "[TG] 第 %d/%d 片请求异常: %s", idx + 1, len(chunks), e
            )
            all_ok = False

    session.close()
    return all_ok


def _split_text(text: str, max_len: int) -> list[str]:
    """按最大长度分割文本，优先在换行处断开。"""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + max_len
        if end >= len(text):
            chunks.append(text[start:])
            break

        # 尝试在 max_len 之前的最后一个换行处分段
        cut = text.rfind("\n", start, end)
        if cut > start:
            end = cut
        else:
            # 没有换行，直接按字符数截断
            end = start + max_len

        chunks.append(text[start:end])
        start = end

    return chunks


def send_square(text: str, api_key: str | None = None) -> bool:
    """发送消息到 Binance Square。

    Parameters
    ----------
    text : str
        消息内容。
    api_key : str | None
        Square API Key，为空时从 config.SQUARE_API_KEY 读取。

    Returns
    -------
    bool
        全部发送成功返回 True，否则返回 False。
    """
    from .config import SQUARE_API_KEY
    key = api_key or SQUARE_API_KEY
    if not key:
        logger.warning("[Square] 未配置 SQUARE_API_KEY，跳过发布")
        return False

    from providers.binance_square import BinanceSquareProvider
    provider = BinanceSquareProvider(api_key=key, dry_run=False)

    # Binance Square 单条限制约 8000 字符，按 7000 分片留余量
    chunks = _split_text(text, 7000)
    all_ok = True
    for idx, chunk in enumerate(chunks):
        result = provider.create_post(chunk)
        if result["success"]:
            url = result.get("url") or "(无链接)"
            logger.info("[Square] 第 %d/%d 片发布成功: %s", idx + 1, len(chunks), url)
        else:
            logger.error("[Square] 第 %d/%d 片发布失败: %s", idx + 1, len(chunks), result.get("message", ""))
            all_ok = False

    return all_ok


def send_test_message() -> bool:
    """发送一条测试消息到 Telegram。

    Returns
    -------
    bool
        发送成功返回 True，否则返回 False。
    """
    return send_telegram("🧪 抓庄雷达测试消息 — 如果收到说明 TG 推送正常")
