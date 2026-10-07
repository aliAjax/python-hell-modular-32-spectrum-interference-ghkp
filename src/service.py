from . import domain, rules
from .domain import ConflictError, DomainError
from .merging import action_dedupe_key


class Service:
    def __init__(self, repository):
        self.repository = repository

    def _identity(self, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)

    def _require_region(self, item, region, role, message="不能处理其他区域的记录"):
        if rules.ENFORCE_REGION and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", message, 403)

    def create_item(self, payload, actor, role, region=None):
        self._identity(actor, role)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        if region and role != "regulator" and normalized["payload"].get("region") != region:
            raise DomainError("region_mismatch", "不能在其他管辖区域创建事件", 403)
        stable_key = normalized.pop("_stable_key")
        result = self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized["payload"], actor, role,
            source_no=normalized.get("source_no"),
            client_ref=normalized.get("client_ref"),
            client_group_no=normalized.get("client_group_no"),
        )
        self.repository.rebuild_merge_groups()
        return self.get_item(result["id"])

    def add_source(self, item_id, payload, actor, role, region=None):
        self._identity(actor, role)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        if item["merge_status"] == "merged":
            raise ConflictError("item_merged", "来源事件已归并，请向主事件提交测量记录")
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized["payload"].get("region") \
                and normalized["payload"]["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        self._require_region(item, region, role, "不能向其他管辖区域的事件提交测量记录")
        existing = self.repository.find_active_source(normalized["source_type"], normalized["external_id"])
        if existing is not None:
            result = dict(existing)
            result["duplicated"] = True
            return result
        result = self.repository.add_source(
            item_id,
            normalized["source_type"],
            normalized["external_id"],
            normalized["payload"],
            normalized["observed_at"],
            actor,
            role,
            origin_item_id=item_id,
            group_no=item.get("merge_group_no"),
        )
        self.repository.rebuild_merge_groups()
        result["duplicated"] = False
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        self._identity(actor, role)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if item["merge_status"] == "merged" and action != "cancel":
            raise DomainError("item_merged", "原事件已归并，处置动作只能提交到主事件", 409)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            self._require_region(item, region, role, "不能处理其他区域的记录")
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        if action in rules.MERGE_RECALCULATE_ACTIONS:
            self.repository.rebuild_merge_groups()
        return self.get_item(item_id)

    def queue_action(self, item_id, action, payload, actor, role, region=None):
        self._identity(actor, role)
        if action not in rules.ACTION_ROLES:
            raise DomainError("unknown_action", "不支持的操作")
        if role not in rules.ACTION_ROLES[action]:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        item = self.repository.get_item(item_id)
        if item["merge_status"] == "merged":
            raise DomainError("item_merged", "原事件已归并，未执行动作需提交到主事件", 409)
        self._require_region(item, region, role, "不能处理其他区域的记录")
        dedupe_key = domain.optional_text(payload, "client_action_id") or action_dedupe_key(action, payload)
        return self.repository.queue_pending_action(
            item_id, action, actor, role, payload, dedupe_key=dedupe_key
        )

    def execute_pending_action(self, pending_id, payload, actor, role, expected_version=None, region=None):
        self._identity(actor, role)
        pending = self.repository.get_pending_action(pending_id)
        action = pending["action"]
        if role not in rules.ACTION_ROLES.get(action, set()):
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if role != pending["role"] and role != "coordinator" and role != "regulator":
            raise DomainError("forbidden", "不能执行其他角色登记的处置动作", 403)
        item = self.repository.get_item(pending["item_id"])
        if action in rules.REGION_SENSITIVE_ACTIONS:
            self._require_region(item, region, role, "不能处理其他区域的记录")
        if pending["status"] not in ("queued", "transferred"):
            raise ConflictError("pending_action_not_open", "未执行动作已处理，不能重复执行")
        if expected_version is None:
            raise DomainError("expected_version_required", "执行未执行动作需要 expected_version", 400)
        queued_payload = dict(pending["payload"])
        queued_payload.update(payload or {})
        try:
            new_status, new_payload, event_payload = rules.apply_action(
                item, action, queued_payload, pending["actor"], pending["role"]
            )
        except DomainError:
            raise
        self.repository.mark_pending_action_executed(
            pending_id, item["id"], action, pending["actor"], pending["role"],
            new_status, new_payload, event_payload, expected_version
        )
        if action in rules.MERGE_RECALCULATE_ACTIONS:
            self.repository.rebuild_merge_groups()
        return self.get_item(item["id"])

    def designate_master(self, group_no, payload, actor, role, region=None, expected_version=None):
        self._identity(actor, role)
        if role != "coordinator":
            raise DomainError("forbidden", "只有协调人可以归并待归并事件", 403)
        if expected_version is None:
            raise DomainError("expected_version_required", "归并操作需要待归并组 version", 400)
        master_item_id = payload.get("master_item_id")
        if isinstance(master_item_id, bool) or not isinstance(master_item_id, int):
            raise DomainError("field_required", "master_item_id 必须是整数")
        group = self.repository.get_merge_group(group_no)
        if region and group.get("region") != region:
            raise DomainError("region_mismatch", "不能归并其他管辖区域的事件", 403)
        result = self.repository.designate_master(group_no, master_item_id, actor, role, expected_version)
        return result

    def rebuild_pending_groups(self, actor=None, role=None, region=None):
        groups = self.repository.rebuild_merge_groups()
        if region:
            groups = [group for group in groups if group["region"] == region]
        return {"groups": groups}

    def sync_group(self, payload, actor, role, region=None):
        self._identity(actor, role)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能同步离线台账", 403)
        group_no = domain.require_text(payload, "group_no")
        reports = payload.get("reports")
        if not isinstance(reports, list) or not reports:
            raise DomainError("field_required", "reports 必须是非空数组")

        created_items = []
        item_refs = {}
        result = {"group_no": group_no, "items": [], "measurements": [], "actions": [], "duplicates": 0}

        for report in reports:
            if not isinstance(report, dict):
                raise DomainError("invalid_report", "每条上报必须是对象")
            report_payload = dict(report.get("payload") or report)
            report_payload["group_no"] = group_no
            report_ref = domain.optional_text(report, "client_ref") or domain.optional_text(report_payload, "client_ref")
            if not report_ref:
                raise DomainError("field_required", "每条离线事件需要 client_ref")
            report_payload.setdefault("client_ref", report_ref)
            normalized_report = domain.normalize_create(report_payload)
            stable_key = normalized_report.pop("_stable_key")
            existing = self._find_item_by_client_ref(group_no, report_ref)
            if existing is None:
                existing = self.repository.find_item_by_stable_key(rules.ENTITY_TYPE, stable_key)
            if existing:
                if existing.get("client_group_no") and existing["client_group_no"] != group_no:
                    raise ConflictError("duplicate_other_group", "重复事件已属于其他离线组")
                if not existing.get("client_ref"):
                    existing = self.repository.attach_client_ref(
                        existing["id"], group_no, report_ref, actor, role
                    )
                created_items.append(existing)
                item_refs[report_ref] = existing["id"]
                result["duplicates"] += 1
                continue
            item = self.create_item(report_payload, actor, role, region)
            created_items.append(item)
            item_refs[report_ref] = item["id"]

        for item in created_items:
            self._require_region(item, region, role, "不能同步其他管辖区域的事件")

        for measurement in payload.get("measurements", []):
            if not isinstance(measurement, dict):
                raise DomainError("invalid_measurement", "测量记录必须是对象")
            report_ref = domain.require_text(measurement, "report_ref")
            item_id = item_refs.get(report_ref)
            if item_id is None:
                raise DomainError("unknown_report_ref", "测量记录引用了不存在的离线事件")
            normalized = domain.normalize_source(measurement)
            existing = self.repository.find_active_source(normalized["source_type"], normalized["external_id"])
            if existing:
                result["measurements"].append(dict(existing, duplicated=True))
                result["duplicates"] += 1
                continue
            item = self.repository.get_item(item_id)
            saved = self.repository.add_source(
                item_id, normalized["source_type"], normalized["external_id"], normalized["payload"],
                normalized["observed_at"], actor, role, origin_item_id=item_id,
                group_no=item.get("merge_group_no"),
            )
            saved["duplicated"] = False
            result["measurements"].append(saved)

        self.repository.rebuild_merge_groups()

        for queued in payload.get("actions", []):
            if not isinstance(queued, dict):
                raise DomainError("invalid_action", "动作记录必须是对象")
            report_ref = domain.require_text(queued, "report_ref")
            origin_id = item_refs.get(report_ref)
            if origin_id is None:
                raise DomainError("unknown_report_ref", "动作引用了不存在的离线事件")
            origin = self.repository.get_item(origin_id)
            action = domain.require_text(queued, "action")
            if action not in rules.ACTION_ROLES:
                raise DomainError("unknown_action", "不支持的操作")
            action_role = domain.optional_text(queued, "role") or role
            action_actor = domain.optional_text(queued, "actor") or actor
            if action_role not in rules.ACTION_ROLES[action]:
                raise DomainError("forbidden", "离线动作登记的角色无权执行该操作", 403)
            action_payload = dict(queued.get("payload") or {})
            client_action_id = domain.optional_text(queued, "client_action_id")
            action_payload["client_action_id"] = client_action_id
            if origin["merge_status"] == "merged" and origin["master_item_id"]:
                target_id = origin["master_item_id"]
            else:
                target_id = origin_id
            saved = self.repository.queue_pending_action(
                origin_id, action, action_actor, action_role, action_payload,
                dedupe_key=client_action_id or action_dedupe_key(action, action_payload),
                origin_item_id=origin_id, target_item_id=target_id,
            )
            result["actions"].append(saved)
            if saved.get("duplicated"):
                result["duplicates"] += 1

        result["items"] = [self.get_item(item_id) for item_id in item_refs.values()]
        group = self.repository.get_merge_group(group_no) if self._group_exists(group_no) else None
        result["group"] = group
        return result

    def _group_exists(self, group_no):
        try:
            self.repository.get_merge_group(group_no)
            return True
        except DomainError as exc:
            if exc.code == "merge_group_not_found":
                return False
            raise

    def _find_item_by_client_ref(self, group_no, client_ref):
        for item in self.repository.list_items():
            if item.get("client_group_no") == group_no and item.get("client_ref") == client_ref:
                return item
        return None

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["actions"] = self.repository.list_actions(item_id)
        item["pending_actions"] = self.repository.list_pending_actions(item_id)
        item["merge_action_refs"] = self.repository.list_merge_action_refs(
            master_item_id=item_id
        ) if item.get("merge_status") == "master" else []
        item["merge_group"] = self.repository.get_merge_group(item["merge_group_no"]) if item.get("merge_group_no") else None
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def list_merge_groups(self, status=None):
        return self.repository.list_merge_groups(status)

    def state(self):
        state = self.repository.state_summary()
        state["pending_groups"] = self.repository.list_merge_groups("pending")
        return state
