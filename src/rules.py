import math
from datetime import datetime, timezone

from .domain import DomainError

ENTITY_TYPE = "spectrum_interference"
INITIAL_STATUS = "pending"
CREATE_ROLES = {"analyst", "monitor"}
SOURCE_ROLES = {"analyst", "monitor", "field_operator"}
ACTION_ROLES = {
    "assess": {"analyst", "monitor"},
    "locate": {"field_operator", "analyst"},
    "suspend": {"coordinator", "regulator"},
    "coordinate": {"coordinator"},
    "resolve": {"coordinator", "regulator"},
    "correct_measurement": {"analyst", "monitor"},
    "cancel": {"coordinator"},
}
ENFORCE_REGION = True
REGION_SENSITIVE_ACTIONS = {"suspend", "coordinate", "resolve", "cancel"}
ACTION_REQUIRES_VERSION = {"suspend", "coordinate", "resolve", "cancel"}


def assess(payload):
    strength = float(payload.get("strength_dbm", -120))
    bandwidth = max(float(payload.get("bandwidth_mhz", 0.1)), 0.001)
    impact = strength + 10.0 * math.log10(bandwidth * 1000.0)
    if impact >= -37:
        level = "critical"
    elif impact >= -50:
        level = "high"
    elif impact >= -65:
        level = "medium"
    else:
        level = "low"
    score = round(max(0.0, min(100.0, 100.0 + impact)), 2)
    return {"score": score, "level": level, "impact_value": round(impact, 2)}


def _need_status(item, allowed):
    if item["status"] not in allowed:
        raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % item["status"])


def _text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def apply_action(item, action, payload, actor, role):
    status = item["status"]
    current = dict(item["payload"])

    if action == "assess":
        _need_status(item, {"pending", "assessed"})
        current["assessment"] = assess(current)
        return "assessed", current, {"assessment": current["assessment"]}

    if action == "correct_measurement":
        _need_status(item, {"pending", "assessed", "located"})
        try:
            strength = float(payload["strength_dbm"])
        except (KeyError, TypeError, ValueError):
            raise DomainError("field_required", "strength_dbm 不能为空")
        revision = {
            "old_strength_dbm": current.get("strength_dbm"),
            "new_strength_dbm": strength,
            "reason": _text(payload, "reason"),
            "actor": actor,
        }
        current.setdefault("measurement_revisions", []).append(revision)
        current["strength_dbm"] = strength
        current["assessment"] = assess(current)
        return status, current, {"revision": revision}

    if action == "locate":
        _need_status(item, {"assessed", "located"})
        location = _text(payload, "location")
        confidence = float(payload.get("confidence", 0))
        if confidence < 0.6:
            raise DomainError("low_location_confidence", "定位置信度低于0.6，不能进入处置", 409)
        current["location"] = {"label": location, "confidence": confidence}
        return "located", current, {"location": current["location"]}

    if action == "suspend":
        _need_status(item, {"located", "suspended"})
        authorization = _text(payload, "authorization_code")
        if not authorization.startswith("REG-"):
            raise DomainError("invalid_authorization", "停用授权编号无效", 403)
        current["suspend_authorization"] = authorization
        return "suspended", current, {"authorization_code": authorization}

    if action == "coordinate":
        _need_status(item, {"suspended"})
        agreement = _text(payload, "coordination_agreement")
        current["coordination_agreement"] = agreement
        current["coordination_note"] = payload.get("note", "")
        return "coordinating", current, {"coordination_agreement": agreement}

    if action == "resolve":
        _need_status(item, {"coordinating"})
        if not payload.get("measurement_cleared"):
            raise DomainError("interference_present", "干扰尚未消除，不能结案", 409)
        current["resolution"] = {"evidence": _text(payload, "evidence"), "cleared": True}
        return "resolved", current, {"evidence": current["resolution"]["evidence"]}

    if action == "cancel":
        _need_status(item, {"pending", "assessed"})
        reason = _text(payload, "reason")
        current["cancellation"] = {"reason": reason, "actor": actor}
        return "cancelled", current, {"reason": reason}

    raise DomainError("unknown_action", "不支持的操作")


# ---------------------------------------------------------------------------
# 归并账：重复上报的候选判定与归并规则
# ---------------------------------------------------------------------------

MERGE_TIME_WINDOW_SECONDS = 15 * 60
MERGE_FREQ_TOLERANCE_MHZ = 2.0
MERGE_ROLES = {"coordinator"}
MERGE_GROUP_STATUSES = {"pending", "merged", "invalidated"}


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _epoch(value):
    dt = _parse_ts(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def freq_band(frequency_mhz):
    """同一频段的分桶（按 MHz 取整），用于归并组号。"""
    try:
        return int(round(float(frequency_mhz)))
    except (TypeError, ValueError):
        return 0


def time_bucket(detected_at):
    """十五分钟时间桶，用于归并组号。"""
    epoch = _epoch(detected_at)
    if epoch is None:
        return 0
    return int(epoch // MERGE_TIME_WINDOW_SECONDS)


def is_candidate_duplicate(a, b):
    """两条事件是否疑似同一干扰：同频段、同地点、十五分钟内重复上报。"""
    pa = a.get("payload", {})
    pb = b.get("payload", {})
    if pa.get("region") != pb.get("region"):
        return False
    if not pa.get("station_id") or pa.get("station_id") != pb.get("station_id"):
        return False
    try:
        fa = float(pa.get("frequency_mhz", 0))
        fb = float(pb.get("frequency_mhz", 0))
    except (TypeError, ValueError):
        return False
    if abs(fa - fb) > MERGE_FREQ_TOLERANCE_MHZ:
        return False
    ta = _epoch(pa.get("detected_at"))
    tb = _epoch(pb.get("detected_at"))
    if ta is None or tb is None:
        return False
    if abs(ta - tb) > MERGE_TIME_WINDOW_SECONDS:
        return False
    return True


def merge_group_key(item):
    """稳定的归并组号：同频段、同地点、同一十五分钟桶。"""
    payload = item.get("payload", {})
    return "mg|%s|%s|%s|%s" % (
        payload.get("region", ""),
        payload.get("station_id", ""),
        freq_band(payload.get("frequency_mhz")),
        time_bucket(payload.get("detected_at")),
    )


def validate_merge(group, main_item, actor, role, region):
    if not actor or not role:
        raise DomainError("identity_required", "需要用户身份和角色", 401)
    if role not in MERGE_ROLES:
        raise DomainError("forbidden", "只有协调人可以归并事件", 403)
    if group["status"] != "pending":
        raise DomainError("group_not_pending", "归并组已处理，不能重复归并", 409)
    if main_item is None:
        raise DomainError("main_item_required", "必须指定主事件", 400)
    if role != "regulator" and region and group["region"] != region:
        raise DomainError("region_mismatch", "不能归并其他辖区的事件", 403)
    return True


def validate_planned_action(role):
    allowed = {r for roles in ACTION_ROLES.values() for r in roles}
    if role not in allowed:
        raise DomainError("forbidden", "当前角色不能登记拟办动作", 403)
    return True
