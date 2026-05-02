from dataclasses import dataclass, field
from typing import Any, Callable


class DuplicateCapabilityError(ValueError):
    """重复注册 capability。"""


class CapabilityNotFoundError(KeyError):
    """未找到 capability。"""


class PayloadValidationError(ValueError):
    """payload 未通过最小契约校验。"""


@dataclass
class Capability:
    name: str
    description: str
    handler: Callable[[dict[str, Any]], Any]
    input_schema: dict[str, Any] = field(
        default_factory=lambda: {
            "type": "object",
            "required": [],
            "properties": {},
            "additionalProperties": False,
        }
    )
    risk_level: str = "read"
    enabled: bool = True
    provider: str = ""
    requires_approval: bool = False


class CapabilityRegistry:
    def __init__(self):
        self._capabilities: dict[str, Capability] = {}

    def register(self, capability: Capability) -> Capability:
        if capability.name in self._capabilities:
            raise DuplicateCapabilityError(f"capability already registered: {capability.name}")
        self._capabilities[capability.name] = capability
        return capability

    def get(self, name: str) -> Capability:
        try:
            return self._capabilities[name]
        except KeyError as exc:
            raise CapabilityNotFoundError(name) from exc

    def list_enabled(self) -> list[Capability]:
        return [cap for cap in self._capabilities.values() if cap.enabled]

    def invoke(self, name: str, payload: dict[str, Any]) -> Any:
        capability = self.get(name)
        if not capability.enabled:
            raise PayloadValidationError(f"capability disabled: {name}")
        self._validate_payload(payload, capability.input_schema)
        return capability.handler(payload)

    def _validate_payload(self, payload: Any, schema: dict[str, Any] | None) -> None:
        schema = schema or {}
        expected_type = schema.get("type", "object")
        if expected_type == "object":
            if not isinstance(payload, dict):
                raise PayloadValidationError("payload 必须是对象")
            properties = schema.get("properties", {}) or {}
            required = schema.get("required", []) or []
            additional_properties = schema.get("additionalProperties", False)

            for field_name in required:
                if field_name not in payload:
                    raise PayloadValidationError(f"缺少必填字段: {field_name}")

            for key in payload:
                if key not in properties and not additional_properties:
                    raise PayloadValidationError(f"不允许的字段: {key}")

            for key, rules in properties.items():
                if key not in payload:
                    continue
                self._validate_value(key, payload[key], rules or {})
            return

        raise PayloadValidationError(f"不支持的 schema type: {expected_type}")

    def _validate_value(self, key: str, value: Any, rules: dict[str, Any]) -> None:
        value_type = rules.get("type")
        if value_type == "string":
            if not isinstance(value, str):
                raise PayloadValidationError(f"字段 {key} 必须为字符串")
            min_length = rules.get("minLength")
            if min_length is not None and len(value) < min_length:
                raise PayloadValidationError(f"字段 {key} 长度不能小于 {min_length}")
