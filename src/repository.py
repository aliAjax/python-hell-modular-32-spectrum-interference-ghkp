import json
import sqlite3
from datetime import datetime, timezone

from . import rules
from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS merge_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    main_item_id INTEGER,
                    region TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    merged_at TEXT
                );
                CREATE TABLE IF NOT EXISTS merge_group_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    role TEXT NOT NULL DEFAULT 'member',
                    joined_at TEXT NOT NULL,
                    UNIQUE(group_id, item_id),
                    FOREIGN KEY(group_id) REFERENCES merge_groups(id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS planned_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'planned',
                    action_key TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, action_key),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                """
            )
            self._ensure_columns(conn)
        finally:
            conn.close()

    def _ensure_columns(self, conn):
        existing = {row[1] for row in conn.execute("PRAGMA table_info(actions)").fetchall()}
        if "action_key" not in existing:
            conn.execute("ALTER TABLE actions ADD COLUMN action_key TEXT")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_actions_action_key ON actions(action_key) "
            "WHERE action_key IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_merge_groups_key ON merge_groups(group_key)"
        )

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def list_actions(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM actions WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 归并账
    # ------------------------------------------------------------------

    def _row_to_group(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def find_item_by_stable_key(self, stable_key):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM items WHERE entity_type=? AND stable_key=?",
                (rules.ENTITY_TYPE, stable_key),
            ).fetchone()
            return self._row_to_item(row)
        finally:
            conn.close()

    def _member_rows(self, conn, group_id):
        rows = conn.execute(
            "SELECT * FROM merge_group_members WHERE group_id=? ORDER BY id", (group_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def list_merge_groups(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute(
                    "SELECT * FROM merge_groups WHERE status=? ORDER BY id DESC", (status,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM merge_groups ORDER BY id DESC").fetchall()
            groups = []
            for row in rows:
                group = self._row_to_group(row)
                group["members"] = self._members_with_items(conn, group["id"])
                groups.append(group)
            return groups
        finally:
            conn.close()

    def _members_with_items(self, conn, group_id):
        members = []
        for member in self._member_rows(conn, group_id):
            item = conn.execute("SELECT * FROM items WHERE id=?", (member["item_id"],)).fetchone()
            summary = None
            if item is not None:
                payload = json.loads(item["payload"])
                summary = {
                    "item_id": item["id"],
                    "status": item["status"],
                    "role": member["role"],
                    "station_id": payload.get("station_id"),
                    "region": payload.get("region"),
                    "frequency_mhz": payload.get("frequency_mhz"),
                    "detected_at": payload.get("detected_at"),
                    "strength_dbm": payload.get("strength_dbm"),
                }
            members.append(summary)
        return members

    def get_merge_group(self, group_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM merge_groups WHERE id=?", (group_id,)).fetchone()
            if row is None:
                raise NotFoundError("group_not_found", "归并组不存在")
            group = self._row_to_group(row)
            group["members"] = self._members_with_items(conn, group_id)
            return group
        finally:
            conn.close()

    def _find_pending_group_for_item(self, conn, item_id):
        row = conn.execute(
            "SELECT g.* FROM merge_groups g JOIN merge_group_members m ON m.group_id=g.id "
            "WHERE g.status='pending' AND m.item_id=? LIMIT 1",
            (item_id,),
        ).fetchone()
        return self._row_to_group(row)

    def consider_for_grouping(self, item):
        """重复上报检测：同频段、同地点、十五分钟内 → 进入待归并区。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            target = conn.execute("SELECT * FROM items WHERE id=?", (item["id"],)).fetchone()
            if target is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            target = self._row_to_item(target)
            existing = self._find_pending_group_for_item(conn, target["id"])
            if existing is not None:
                conn.execute("COMMIT")
                return existing

            region = target["payload"].get("region")
            candidates = conn.execute(
                "SELECT * FROM merge_groups WHERE status='pending' AND region=? ORDER BY id",
                (region,),
            ).fetchall()
            for row in candidates:
                group = self._row_to_group(row)
                for member in self._member_rows(conn, group["id"]):
                    other = self._row_to_item(
                        conn.execute("SELECT * FROM items WHERE id=?", (member["item_id"],)).fetchone()
                    )
                    if other is not None and rules.is_candidate_duplicate(other, target):
                        conn.execute(
                            "INSERT OR IGNORE INTO merge_group_members(group_id,item_id,role,joined_at) "
                            "VALUES(?,?,'member',?)",
                            (group["id"], target["id"], now_iso()),
                        )
                        conn.execute("COMMIT")
                        return self.get_merge_group(group["id"])

            group_key = rules.merge_group_key(target)
            try:
                conn.execute(
                    "INSERT INTO merge_groups(group_key,status,main_item_id,region,version,payload,created_at,updated_at) "
                    "VALUES(?, 'pending', NULL, ?, 1, ?, ?, ?)",
                    (group_key, region, canonical_json({"anchor_item_id": target["id"]}), now_iso(), now_iso()),
                )
            except sqlite3.IntegrityError:
                row = conn.execute("SELECT * FROM merge_groups WHERE group_key=?", (group_key,)).fetchone()
                group = self._row_to_group(row)
                conn.execute(
                    "INSERT OR IGNORE INTO merge_group_members(group_id,item_id,role,joined_at) "
                    "VALUES(?,?,'member',?)",
                    (group["id"], target["id"], now_iso()),
                )
                conn.execute("COMMIT")
                return self.get_merge_group(group["id"])
            group_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            conn.execute(
                "INSERT INTO merge_group_members(group_id,item_id,role,joined_at) VALUES(?,?,'member',?)",
                (group_id, target["id"], now_iso()),
            )
            self.append_audit(conn, target["id"], "entered_pending_area", "system", "system", {"group_id": group_id})
            conn.execute("COMMIT")
            return self.get_merge_group(group_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def merge_group(self, group_id, main_item_id, actor, role, region, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM merge_groups WHERE id=?", (group_id,)).fetchone()
            if row is None:
                raise NotFoundError("group_not_found", "归并组不存在")
            group = self._row_to_group(row)
            main_row = conn.execute("SELECT * FROM items WHERE id=?", (main_item_id,)).fetchone()
            main_item = self._row_to_item(main_row)
            if expected_version is None:
                raise DomainError("expected_version_required", "归并需要 expected_version", 400)
            if int(expected_version) != int(group["version"]):
                raise ConflictError("version_conflict", "归并组已被其他协调人处理，请重新读取")
            rules.validate_merge(group, main_item, actor, role, region)

            member_ids = [m["item_id"] for m in self._member_rows(conn, group_id)]
            if main_item_id not in member_ids:
                raise DomainError("main_not_in_group", "主事件必须是归并组成员", 400)

            main_sources = {
                (r["source_type"], r["external_id"])
                for r in conn.execute(
                    "SELECT source_type,external_id FROM sources WHERE item_id=?", (main_item_id,)
                ).fetchall()
            }
            transferred_sources = []
            transferred_planned = []
            merged_members = []

            for member_id in member_ids:
                if member_id == main_item_id:
                    conn.execute(
                        "UPDATE merge_group_members SET role='main' WHERE group_id=? AND item_id=?",
                        (group_id, member_id),
                    )
                    continue

                # 测量记录整批转过去；来源编号（source_type+external_id）相同的重复记录保留在原事件
                for src in conn.execute("SELECT * FROM sources WHERE item_id=?", (member_id,)).fetchall():
                    key = (src["source_type"], src["external_id"])
                    if key in main_sources:
                        continue
                    conn.execute("UPDATE sources SET item_id=? WHERE id=?", (main_item_id, src["id"]))
                    main_sources.add(key)
                    transferred_sources.append(src["id"])

                # 未执行（拟办）动作整批转过去；已执行动作只留记录，不再重放
                for planned in conn.execute(
                    "SELECT * FROM planned_actions WHERE item_id=? AND status='planned'", (member_id,)
                ).fetchall():
                    conn.execute(
                        "UPDATE planned_actions SET item_id=?, status='transferred' WHERE id=?",
                        (main_item_id, planned["id"]),
                    )
                    transferred_planned.append(planned["id"])

                member = self._row_to_item(
                    conn.execute("SELECT * FROM items WHERE id=?", (member_id,)).fetchone()
                )
                new_payload = dict(member["payload"])
                new_payload["merged_into"] = main_item_id
                new_payload["merged_from_status"] = member["status"]
                conn.execute(
                    "UPDATE items SET status='merged', payload=?, version=version+1, updated_at=? WHERE id=?",
                    (canonical_json(new_payload), now_iso(), member_id),
                )
                conn.execute(
                    "UPDATE merge_group_members SET role='merged' WHERE group_id=? AND item_id=?",
                    (group_id, member_id),
                )
                self.append_audit(conn, member_id, "merged_into", actor, role, {"group_id": group_id, "main_item_id": main_item_id})
                merged_members.append(member_id)

            conn.execute(
                "UPDATE merge_groups SET status='merged', main_item_id=?, version=version+1, updated_at=?, merged_at=? WHERE id=?",
                (main_item_id, now_iso(), now_iso(), group_id),
            )
            self.append_audit(
                conn,
                main_item_id,
                "merge_completed",
                actor,
                role,
                {
                    "group_id": group_id,
                    "merged_members": merged_members,
                    "transferred_sources": transferred_sources,
                    "transferred_planned_actions": transferred_planned,
                },
            )
            conn.execute("COMMIT")
            return self.get_merge_group(group_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def recalculate_for_item(self, item_id):
        """主事件状态或管辖区域变更后：待归并选点与转办动作失效重算。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            groups = conn.execute(
                "SELECT * FROM merge_groups WHERE main_item_id=? AND status='merged'", (item_id,)
            ).fetchall()
            changed = []
            for row in groups:
                group = self._row_to_group(row)
                conn.execute(
                    "UPDATE merge_groups SET status='invalidated', version=version+1, updated_at=? WHERE id=?",
                    (now_iso(), group["id"]),
                )
                # 已转办的拟办动作退回待办，重新参与选点
                conn.execute(
                    "UPDATE planned_actions SET status='planned' WHERE item_id=? AND status='transferred'",
                    (item_id,),
                )
                self.append_audit(conn, item_id, "group_recalculated", "system", "system", {"group_id": group["id"]})
                changed.append(group["id"])
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        if changed:
            fresh = self.get_item(item_id)
            self.consider_for_grouping(fresh)
        return changed

    def create_planned_action(self, item_id, action, payload, actor, role, action_key=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO planned_actions(item_id,action,payload,actor,role,status,action_key,created_at) "
                    "VALUES(?,?,?,?,?, 'planned', ?, ?)",
                    (item_id, action, canonical_json(payload), actor, role, action_key, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_planned_action", "同一拟办动作已经登记")
            planned_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "planned_action_recorded",
                actor,
                role,
                {"planned_action_id": planned_id, "action": action},
            )
            conn.execute("COMMIT")
            return self.get_planned_action(planned_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_planned_action(self, planned_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM planned_actions WHERE id=?", (planned_id,)).fetchone()
            if row is None:
                raise NotFoundError("planned_action_not_found", "拟办动作不存在")
            result = dict(row)
            result["payload"] = json.loads(result["payload"])
            return result
        finally:
            conn.close()

    def list_planned_actions(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM planned_actions WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action_record(self, item_id, action, actor, role, payload, action_key=None, group_no=None):
        """断网回网：按组号合并动作记录，重复不新增动作。不重放状态机。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                conn.execute("COMMIT")
                return None
            if action_key is not None:
                exists = conn.execute(
                    "SELECT id FROM actions WHERE action_key=? LIMIT 1", (action_key,),
                ).fetchone()
                if exists is not None:
                    conn.execute("COMMIT")
                    return None
            try:
                conn.execute(
                    "INSERT INTO actions(item_id,action,actor,role,payload,action_key,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (item_id, action, actor, role, canonical_json(payload), action_key, now_iso()),
                )
            except sqlite3.IntegrityError:
                conn.execute("COMMIT")
                return None
            action_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "action_synced",
                actor,
                role,
                {"action_id": action_id, "action": action, "group_no": group_no},
            )
            conn.execute("COMMIT")
            return {"id": action_id, "item_id": item_id, "action": action, "action_key": action_key}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
