import time

import requests


POST_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"


class BinanceSquareProvider:
    def __init__(self, api_key: str, dry_run: bool = False, session=None, timeout: int = 15):
        self.api_key = api_key or ""
        self.dry_run = bool(dry_run or not self.api_key)
        self.session = session or requests
        self.timeout = timeout

    def create_post(self, content: str) -> dict:
        if self.dry_run:
            mock_id = f"MOCK_{int(time.time())}"
            return {
                "success": True,
                "code": "000000",
                "post_id": mock_id,
                "url": f"https://www.binance.com/square/post/{mock_id}",
                "message": "dry-run mock success",
                "mock": True,
            }

        try:
            response = self.session.post(
                POST_URL,
                headers={
                    "X-Square-OpenAPI-Key": self.api_key,
                    "Content-Type": "application/json",
                    "clienttype": "binanceSkill",
                },
                json={"bodyTextOnly": content},
                timeout=self.timeout,
            )
        except Exception as exc:
            return {
                "success": False,
                "code": "NETWORK_ERROR",
                "message": self._sanitize(str(exc)),
                "mock": False,
            }

        try:
            data = response.json()
        except Exception:
            snippet = self._sanitize((getattr(response, "text", "") or "").strip().replace("\n", " ")[:200])
            return {
                "success": False,
                "code": f"HTTP_{getattr(response, 'status_code', 'UNKNOWN')}_NON_JSON",
                "message": snippet or "接口返回非 JSON 响应",
                "mock": False,
            }

        if not isinstance(data, dict):
            return {
                "success": False,
                "code": f"HTTP_{getattr(response, 'status_code', 'UNKNOWN')}_INVALID_JSON",
                "message": "接口返回了非对象 JSON 结构",
                "mock": False,
            }

        code = str(data.get("code", ""))
        if code == "000000":
            payload = data.get("data") or {}
            if not isinstance(payload, dict):
                payload = {}
            post_id = str(payload.get("id") or "")
            if post_id:
                return {
                    "success": True,
                    "code": code,
                    "post_id": post_id,
                    "url": f"https://www.binance.com/square/post/{post_id}",
                    "message": self._sanitize(data.get("message", "success")),
                    "mock": False,
                }
            return {
                "success": True,
                "code": code,
                "post_id": "",
                "url": None,
                "message": "接口返回成功但无返回ID，请到币安广场后台确认是否已成功发帖",
                "mock": False,
            }

        return {
            "success": False,
            "code": code or f"HTTP_{getattr(response, 'status_code', 'UNKNOWN')}",
            "message": self._sanitize(data.get("message", "未知错误")),
            "mock": False,
        }

    def post(self, content: str) -> dict:
        return self.create_post(content)

    def _sanitize(self, text: str) -> str:
        cleaned = str(text or "")
        if self.api_key:
            cleaned = cleaned.replace(self.api_key, "[REDACTED]")
        return cleaned
