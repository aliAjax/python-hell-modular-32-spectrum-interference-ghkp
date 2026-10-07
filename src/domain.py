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


def optional_text(payload, name):
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DomainError("invalid_text", "%s 必须是非空字符串" % name)
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


def optional_number(payload, name, minimum=None, maximum=None):
    if payload.get(name) is None:
        return None
    return number(payload, name, minimum, maximum)


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
    event_payload = {
        "frequency_mhz": frequency,
        "bandwidth_mhz": bandwidth,
        "station_id": station_id,
        "region": region,
        "strength_dbm": strength,
        "detected_at": detected_at,
        "reporter": reporter,
        "measurement_revisions": [],
        "suspend_authorization": None,
    }
    location = optional_text(payload, "location")
    if location:
        event_payload["location"] = location
    latitude = optional_number(payload, "latitude", -90, 90)
    longitude = optional_number(payload, "longitude", -180, 180)
    if latitude is not None:
        event_payload["latitude"] = latitude
    if longitude is not None:
        event_payload["longitude"] = longitude
    return {
        "payload": event_payload,
        "source_no": optional_text(payload, "source_no"),
        "client_ref": optional_text(payload, "client_ref"),
        "client_group_no": optional_text(payload, "group_no") or optional_text(payload, "merge_group_no"),
        "_stable_key": stable_key,
    }


def normalize_source(payload):
    source_type = require_text(payload, "source_type")
    external_id = require_text(payload, "external_id")
    observed_at = parse_timestamp(payload, "observed_at")
    strength = number(payload, "strength_dbm")
    result = {
        "observed_at": observed_at,
        "strength_dbm": strength,
        "region": None,
        "station_id": payload.get("station_id"),
        "frequency_mhz": payload.get("frequency_mhz"),
    }
    region = payload.get("region")
    if region is not None:
        region = str(region).strip() or None
        result["region"] = region
    location = optional_text(payload, "location")
    if location:
        result["location"] = location
    latitude = optional_number(payload, "latitude", -90, 90)
    longitude = optional_number(payload, "longitude", -180, 180)
    if latitude is not None:
        result["latitude"] = latitude
    if longitude is not None:
        result["longitude"] = longitude
    return {
        "source_type": source_type,
        "external_id": external_id,
        "payload": result,
        "observed_at": observed_at,
    }
