from datetime import datetime


class DomainError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.status = status


class ConflictError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 409)


class NotFoundError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 404)


def require_text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def number(payload, name, minimum=None, maximum=None):
    value = payload.get(name)
    if isinstance(value, bool):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    if minimum is not None and value < minimum:
        raise DomainError("invalid_number", "%s 不能小于 %s" % (name, minimum))
    if maximum is not None and value > maximum:
        raise DomainError("invalid_number", "%s 不能大于 %s" % (name, maximum))
    return value


def parse_timestamp(payload, name):
    value = require_text(payload, name)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)
    return value


def normalize_create(payload):
    frequency = number(payload, "frequency_mhz", 0.001, 300000)
    bandwidth = number(payload, "bandwidth_mhz", 0.001)
    station_id = require_text(payload, "station_id")
    region = require_text(payload, "region")
    strength = number(payload, "strength_dbm")
    detected_at = parse_timestamp(payload, "detected_at")
    reporter = require_text(payload, "reporter")
    stable_key = "%s|%s|%s|%s" % (station_id, region, frequency, detected_at)
    return {
        "frequency_mhz": frequency,
        "bandwidth_mhz": bandwidth,
        "station_id": station_id,
        "region": region,
        "strength_dbm": strength,
        "detected_at": detected_at,
        "reporter": reporter,
        "measurement_revisions": [],
        "suspend_authorization": None,
        "_stable_key": stable_key,
    }


def normalize_source(payload):
    source_type = require_text(payload, "source_type")
    external_id = require_text(payload, "external_id")
    observed_at = parse_timestamp(payload, "observed_at")
    strength = number(payload, "strength_dbm")
    region = payload.get("region")
    if region is not None:
        region = str(region).strip() or None
    return {
        "source_type": source_type,
        "external_id": external_id,
        "observed_at": observed_at,
        "strength_dbm": strength,
        "region": region,
        "station_id": payload.get("station_id"),
        "frequency_mhz": payload.get("frequency_mhz"),
    }


def normalize_planned_action(payload):
    action = require_text(payload, "action")
    action_payload = payload.get("payload") or {}
    if not isinstance(action_payload, dict):
        raise DomainError("invalid_payload", "动作载荷必须是对象")
    action_key = payload.get("action_key")
    if action_key is not None:
        action_key = str(action_key).strip() or None
    return {"action": action, "payload": action_payload, "action_key": action_key}


def normalize_sync(payload):
    group_no = require_text(payload, "group_no")
    items = payload.get("items") or []
    actions = payload.get("actions") or []
    if not isinstance(items, list) or not isinstance(actions, list):
        raise DomainError("invalid_sync", "items 和 actions 必须是数组")
    return {"group_no": group_no, "items": items, "actions": actions}
