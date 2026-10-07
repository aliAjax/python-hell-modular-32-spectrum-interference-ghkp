from . import domain, rules
from .domain import ConflictError, DomainError


class Service:
    def __init__(self, repository):
        self.repository = repository

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        item = self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )
        self.repository.consider_for_grouping(item)
        return item

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        old_status = item["status"]
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        if new_status != old_status:
            self.repository.recalculate_for_item(item_id)
        return self.get_item(item_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["actions"] = self.repository.list_actions(item_id)
        item["planned_actions"] = self.repository.list_planned_actions(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

    # ------------------------------------------------------------------
    # 归并账
    # ------------------------------------------------------------------

    def list_merge_groups(self, status=None):
        return self.repository.list_merge_groups(status)

    def get_merge_group(self, group_id):
        return self.repository.get_merge_group(group_id)

    def merge_group(self, group_id, main_item_id, actor, role, region=None, expected_version=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        return self.repository.merge_group(
            group_id, main_item_id, actor, role, region, expected_version
        )

    def create_planned_action(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        rules.validate_planned_action(role)
        normalized = domain.normalize_planned_action(payload)
        item = self.repository.get_item(item_id)
        if (
            rules.ENFORCE_REGION
            and normalized["action"] in rules.REGION_SENSITIVE_ACTIONS
            and region
            and role != "regulator"
            and item["payload"].get("region") != region
        ):
            raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        return self.repository.create_planned_action(
            item_id, normalized["action"], normalized["payload"], actor, role, normalized["action_key"]
        )

    def list_planned_actions(self, item_id):
        return self.repository.list_planned_actions(item_id)

    def sync_batch(self, payload, actor, role, region=None):
        """断网回网：按组号合并，重复不新增动作。"""
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        normalized = domain.normalize_sync(payload)
        group_no = normalized["group_no"]
        items_created = 0
        items_duplicated = 0
        actions_created = 0
        actions_duplicated = 0
        for raw_item in normalized["items"]:
            try:
                self.create_item(raw_item, actor, role, region)
                items_created += 1
            except ConflictError:
                items_duplicated += 1
        for raw_action in normalized["actions"]:
            stable_key = raw_action.get("stable_key")
            if not stable_key:
                continue
            item = self.repository.find_item_by_stable_key(stable_key)
            if item is None:
                continue
            action = raw_action.get("action", "")
            action_key = raw_action.get("action_key")
            result = self.repository.apply_action_record(
                item["id"],
                action,
                actor,
                role,
                raw_action.get("payload") or {},
                action_key,
                group_no,
            )
            if result is None:
                actions_duplicated += 1
            else:
                actions_created += 1
        return {
            "group_no": group_no,
            "items_created": items_created,
            "items_duplicated": items_duplicated,
            "actions_created": actions_created,
            "actions_duplicated": actions_duplicated,
        }
