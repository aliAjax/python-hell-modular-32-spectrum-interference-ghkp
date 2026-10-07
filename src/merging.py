import json
import math
from datetime import datetime, timezone

from .audit import canonical_json


def parse_time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def within_window(first, second, seconds):
    return abs((parse_time(first) - parse_time(second)).total_seconds()) <= seconds


def same_band(first, second):
    f1 = float(first["frequency_mhz"])
    f2 = float(second["frequency_mhz"])
    b1 = max(float(first.get("bandwidth_mhz", 0.001)), 0.001)
    b2 = max(float(second.get("bandwidth_mhz", 0.001)), 0.001)
    return abs(f1 - f2) <= max(b1, b2) / 2.0


def haversine_meters(first, second):
    lat1 = math.radians(float(first["latitude"]))
    lat2 = math.radians(float(second["latitude"]))
    dlat = lat2 - lat1
    dlon = math.radians(float(second["longitude"]) - float(first["longitude"]))
    part = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 6371000.0 * 2.0 * math.asin(math.sqrt(part))


def location_of(value, measurement=None):
    source = measurement or {}
    source_payload = source.get("payload") if isinstance(source, dict) else None
    if not isinstance(source_payload, dict):
        source_payload = {}

    lat = value.get("latitude")
    lon = value.get("longitude")
    if lat is None:
        lat = source_payload.get("latitude")
    if lon is None:
        lon = source_payload.get("longitude")
    if lat is not None and lon is not None:
        return {"latitude": float(lat), "longitude": float(lon)}

    for holder in (source_payload, value):
        location = holder.get("location")
        if isinstance(location, dict) and location.get("label"):
            return {"label": str(location["label"]).strip().lower()}
        if isinstance(location, str) and location.strip():
            return {"label": location.strip().lower()}

    station_ids = {value.get("station_id"), source_payload.get("station_id")}
    station_ids.discard(None)
    if value.get("station_id") and len(station_ids) == 1:
        return {"station_id": str(value["station_id"])}
    return None


def near_location(first, first_measurement, second, second_measurement, max_meters):
    left = location_of(first, first_measurement)
    right = location_of(second, second_measurement)
    if not left or not right:
        return False
    if "latitude" in left and "latitude" in right:
        return haversine_meters(left, right) <= max_meters
    if "label" in left and "label" in right:
        return left["label"] == right["label"]
    if "station_id" in left and "station_id" in right:
        return left["station_id"] == right["station_id"]
    return False


def can_cluster(first, first_measurement, second, second_measurement, window_seconds, nearby_meters):
    return (
        first.get("region") == second.get("region")
        and same_band(first, second)
        and within_window(first["detected_at"], second["detected_at"], window_seconds)
        and near_location(first, first_measurement, second, second_measurement, nearby_meters)
    )


def action_dedupe_key(action, payload):
    if isinstance(payload, dict) and payload.get("client_action_id"):
        return str(payload["client_action_id"]).strip()
    return canonical_json({"action": action, "payload": payload})


def loads(value, default):
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default
