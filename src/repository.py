import json
import sqlite3
import uuid
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError
from .merging import action_dedupe_key, can_cluster


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
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
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
                    group_no TEXT PRIMARY KEY,
                    region TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    anchor_item_id INTEGER,
                    master_item_id INTEGER,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS merge_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_no TEXT NOT NULL,
                    source_item_id INTEGER NOT NULL,
                    source_no TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(group_no) REFERENCES merge_groups(group_no),
                    FOREIGN KEY(source_item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS merge_action_refs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_no TEXT NOT NULL,
                    master_item_id INTEGER NOT NULL,
                    origin_item_id INTEGER NOT NULL,
                    origin_action_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(group_no, origin_action_id),
                    FOREIGN KEY(group_no) REFERENCES merge_groups(group_no),
                    FOREIGN KEY(master_item_id) REFERENCES items(id),
                    FOREIGN KEY(origin_item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS pending_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_no TEXT,
                    item_id INTEGER NOT NULL,
                    origin_item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(origin_item_id, dedupe_key),
                    FOREIGN KEY(item_id) REFERENCES items(id),
                    FOREIGN KEY(origin_item_id) REFERENCES items(id)
                );
                """
            )
            self._ensure_columns(conn, "items", {
                "merge_status": "TEXT NOT NULL DEFAULT 'source'",
                "merge_group_no": "TEXT",
                "master_item_id": "INTEGER",
                "source_no": "TEXT",
                "client_ref": "TEXT",
                "client_group_no": "TEXT",
            })
            self._ensure_columns(conn, "sources", {
                "status": "TEXT NOT NULL DEFAULT 'active'",
                "origin_item_id": "INTEGER",
                "group_no": "TEXT",
            })
            self._ensure_columns(conn, "actions", {
                "status": "TEXT NOT NULL DEFAULT 'executed'",
                "origin_item_id": "INTEGER",
                "group_no": "TEXT",
                "pending_action_id": "INTEGER",
                "dedupe_key": "TEXT",
            })
            conn.executescript(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_items_stable_key ON items(entity_type, stable_key);
                CREATE INDEX IF NOT EXISTS idx_items_merge_group ON items(merge_group_no);
                CREATE INDEX IF NOT EXISTS idx_items_client_group ON items(client_group_no);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_items_source_no ON items(source_no) WHERE source_no IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_items_client_ref
                    ON items(client_group_no, client_ref) WHERE client_ref IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_sources
                    ON sources(source_type, external_id) WHERE status='active';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_item_sources
                    ON sources(item_id, source_type, external_id) WHERE status='active';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_candidates
                    ON merge_candidates(group_no, source_item_id) WHERE status='active';
                CREATE INDEX IF NOT EXISTS idx_candidates_item ON merge_candidates(source_item_id);
                CREATE INDEX IF NOT EXISTS idx_pending_current ON pending_actions(item_id, status);
                CREATE INDEX IF NOT EXISTS idx_pending_origin ON pending_actions(origin_item_id);
                CREATE INDEX IF NOT EXISTS idx_actions_origin ON actions(origin_item_id);
                """
            )
            # Backfill records created before merging existed.
            conn.execute("UPDATE sources SET origin_item_id=item_id WHERE origin_item_id IS NULL")
            conn.execute("UPDATE actions SET origin_item_id=item_id WHERE origin_item_id IS NULL")
        finally:
            conn.close()

    def _ensure_columns(self, conn, table, columns):
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(%s)" % table).fetchall()}
        for name, definition in columns.items():
            if name not in existing:
                conn.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, name, definition))

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _row_to_source(self, row):
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _row_to_action(self, row):
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
        return event["created_at"]

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role,
                    source_no=None, client_ref=None, client_group_no=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,
                                      created_at,updated_at,source_no,client_ref,client_group_no)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
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
                        source_no,
                        client_ref,
                        client_group_no,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise self._integrity_error(exc)
            item_id = cursor.lastrowid
            if source_no is None:
                source_no = "EVT-%06d" % item_id
                conn.execute("UPDATE items SET source_no=? WHERE id=?", (source_no, item_id))
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key, "source_no": source_no})
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

    def _integrity_error(self, exc):
        message = str(exc)
        if "idx_items_client_ref" in message or "client_ref" in message:
            return ConflictError("duplicate_client_ref", "该离线组内的事件编号已经同步")
        if "source_no" in message:
            return ConflictError("duplicate_source_no", "来源编号重复")
        if "idx_active_sources" in message:
            return ConflictError("duplicate_source", "同一测量记录已经提交")
        if "idx_active_candidates" in message:
            return ConflictError("duplicate_candidate", "事件已在待归并组中")
        return ConflictError("duplicate_item", "同一业务实体已经存在")

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def find_item_by_stable_key(self, entity_type, stable_key):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM items WHERE entity_type=? AND stable_key=?", (entity_type, stable_key)
            ).fetchone()
            return self._row_to_item(row) if row else None
        finally:
            conn.close()

    def attach_client_ref(self, item_id, group_no, client_ref, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE items SET client_group_no=?, client_ref=?, updated_at=? WHERE id=?",
                (group_no, client_ref, now_iso(), item_id),
            )
            self.append_audit(conn, item_id, "offline_group_attached", actor, role,
                              {"group_no": group_no, "client_ref": client_ref})
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

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role,
                   origin_item_id=None, group_no=None, status="active"):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            origin_item_id = origin_item_id or item_id
            duplicate = conn.execute(
                "SELECT id, item_id FROM sources WHERE source_type=? AND external_id=? AND status='active'",
                (source_type, external_id),
            ).fetchone()
            if duplicate is not None:
                raise ConflictError("duplicate_source", "同一测量记录已经提交")
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at,
                                        status,origin_item_id,group_no)
                    VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso(),
                     status, origin_item_id, group_no),
                )
            except sqlite3.IntegrityError as exc:
                raise self._integrity_error(exc)
            source_id = cursor.lastrowid
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id,
                 "origin_item_id": origin_item_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type,
                    "external_id": external_id, "payload": payload, "observed_at": observed_at,
                    "status": status, "origin_item_id": origin_item_id, "group_no": group_no}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def find_active_source(self, source_type, external_id):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM sources WHERE source_type=? AND external_id=? AND status='active'",
                (source_type, external_id),
            ).fetchone()
            return self._row_to_source(row) if row else None
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM sources WHERE item_id=? OR origin_item_id=? ORDER BY id DESC",
                (item_id, item_id),
            ).fetchall()
            return [self._row_to_source(row) for row in rows]
        finally:
            conn.close()

    def list_actions(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM actions WHERE item_id=? OR origin_item_id=? ORDER BY id",
                (item_id, item_id),
            ).fetchall()
            return [self._row_to_action(row) for row in rows]
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload,
                     expected_version=None, dedupe_key=None, pending_action_id=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            timestamp = now_iso()
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), timestamp, item_id),
            )
            conn.execute(
                """
                INSERT INTO actions(item_id,action,actor,role,payload,created_at,status,origin_item_id,
                                    group_no,pending_action_id,dedupe_key)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (item_id, action, actor, role, canonical_json(event_payload), timestamp, "executed",
                 item_id, row["merge_group_no"], pending_action_id, dedupe_key),
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

    def queue_pending_action(self, item_id, action, actor, role, payload, dedupe_key=None,
                             origin_item_id=None, target_item_id=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            target_id = target_item_id or item_id
            target = conn.execute("SELECT * FROM items WHERE id=?", (target_id,)).fetchone()
            if target is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            origin_id = origin_item_id or item_id
            origin = conn.execute("SELECT * FROM items WHERE id=?", (origin_id,)).fetchone()
            if origin is None:
                raise NotFoundError("item_not_found", "来源事件不存在")
            if int(target_id) != int(origin_id) and target["merge_status"] not in ("master", "source"):
                raise ConflictError("item_merged", "只能把离线动作转到主事件")
            if target["merge_status"] == "merged" and int(target_id) == int(origin_id):
                raise ConflictError("item_merged", "事件已归并，未执行动作需提交到主事件")
            dedupe_key = dedupe_key or action_dedupe_key(action, payload)
            existing = conn.execute(
                "SELECT * FROM pending_actions WHERE origin_item_id=? AND dedupe_key=?",
                (origin_id, dedupe_key),
            ).fetchone()
            if existing:
                conn.execute("COMMIT")
                pending = self._row_to_action(existing)
                pending["duplicated"] = True
                return pending
            timestamp = now_iso()
            cursor = conn.execute(
                """
                INSERT INTO pending_actions(group_no,item_id,origin_item_id,action,actor,role,payload,
                                            dedupe_key,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (target["merge_group_no"], target_id, origin_id, action, actor, role,
                 canonical_json(payload), dedupe_key, "queued", timestamp, timestamp),
            )
            pending_id = cursor.lastrowid
            self.append_audit(conn, target_id, "pending_action_queued", actor, role,
                              {"pending_action_id": pending_id, "action": action,
                               "dedupe_key": dedupe_key, "origin_item_id": origin_id})
            conn.execute("COMMIT")
            result = self.get_pending_action(pending_id)
            result["duplicated"] = False
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_pending_action(self, pending_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM pending_actions WHERE id=?", (pending_id,)).fetchone()
            if row is None:
                raise NotFoundError("pending_action_not_found", "未执行动作不存在")
            return self._row_to_action(row)
        finally:
            conn.close()

    def list_pending_actions(self, item_id=None, status=None):
        conn = self.connect()
        try:
            sql = "SELECT * FROM pending_actions"
            conditions = []
            params = []
            if item_id is not None:
                conditions.append("(item_id=? OR origin_item_id=?)")
                params.extend([item_id, item_id])
            if status:
                conditions.append("status=?")
                params.append(status)
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += " ORDER BY id"
            return [self._row_to_action(row) for row in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def mark_pending_action_executed(self, pending_id, item_id, action, actor, role, new_status,
                                     new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            pending = conn.execute("SELECT * FROM pending_actions WHERE id=?", (pending_id,)).fetchone()
            if pending is None:
                raise NotFoundError("pending_action_not_found", "未执行动作不存在")
            if pending["status"] not in ("queued", "transferred"):
                raise ConflictError("pending_action_not_open", "未执行动作已处理，不能重复执行")
            if int(pending["item_id"]) != int(item_id):
                raise ConflictError("pending_action_wrong_item", "未执行动作不属于该事件")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            timestamp = now_iso()
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), timestamp, item_id),
            )
            conn.execute(
                """
                INSERT INTO actions(item_id,action,actor,role,payload,created_at,status,origin_item_id,
                                    group_no,pending_action_id,dedupe_key)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (item_id, action, actor, role, canonical_json(event_payload), timestamp, "executed",
                 item_id, row["merge_group_no"], pending_id, pending["dedupe_key"]),
            )
            conn.execute(
                "UPDATE pending_actions SET status='executed', updated_at=? WHERE id=?",
                (timestamp, pending_id),
            )
            self.append_audit(conn, item_id, action, actor, role,
                              dict(event_payload, pending_action_id=pending_id))
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

    def list_merge_action_refs(self, group_no=None, master_item_id=None):
        conn = self.connect()
        try:
            sql = "SELECT * FROM merge_action_refs"
            conditions = []
            params = []
            if group_no:
                conditions.append("group_no=?")
                params.append(group_no)
            if master_item_id:
                conditions.append("master_item_id=?")
                params.append(master_item_id)
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += " ORDER BY id"
            return [self._row_to_action(row) for row in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def get_merge_group(self, group_no):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM merge_groups WHERE group_no=?", (group_no,)).fetchone()
            if row is None:
                raise NotFoundError("merge_group_not_found", "待归并组不存在")
            return self._group_with_candidates(conn, dict(row))
        finally:
            conn.close()

    def list_merge_groups(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM merge_groups WHERE status=? ORDER BY created_at, group_no", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM merge_groups ORDER BY created_at, group_no").fetchall()
            return [self._group_with_candidates(conn, dict(row)) for row in rows]
        finally:
            conn.close()

    def _group_with_candidates(self, conn, group):
        candidates = conn.execute(
            "SELECT * FROM merge_candidates WHERE group_no=? ORDER BY id", (group["group_no"],)
        ).fetchall()
        group["candidates"] = [dict(row) for row in candidates]
        return group

    def rebuild_merge_groups(self):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            active_statuses = ("pending", "assessed", "located", "suspended", "coordinating")
            # detected_at is inside JSON payload; parse rows first and sort in Python.
            rows = conn.execute(
                """
                SELECT * FROM items
                WHERE status IN (%s) AND COALESCE(merge_status, 'source') NOT IN ('merged', 'master')
                """ % ",".join("?" for _ in active_statuses),
                active_statuses,
            ).fetchall()
            items = [self._row_to_item(row) for row in rows]
            items.sort(key=lambda item: (item["payload"]["detected_at"], item["id"]))
            measurements = {}
            for item in items:
                source_rows = conn.execute(
                    "SELECT * FROM sources WHERE origin_item_id=? AND item_id=? ORDER BY observed_at DESC, id DESC",
                    (item["id"], item["id"]),
                ).fetchall()
                measurements[item["id"]] = self._row_to_source(source_rows[0]) if source_rows else None

            parent = list(range(len(items)))

            def find(index):
                while parent[index] != index:
                    parent[index] = parent[parent[index]]
                    index = parent[index]
                return index

            def union(a, b):
                ra, rb = find(a), find(b)
                if ra != rb:
                    parent[max(ra, rb)] = min(ra, rb)

            for i in range(len(items)):
                for j in range(i + 1, len(items)):
                    left_no = items[i].get("client_group_no")
                    right_no = items[j].get("client_group_no")
                    explicit_blocks = left_no and right_no and left_no != right_no
                    if not explicit_blocks and can_cluster(
                        items[i]["payload"], measurements[items[i]["id"]],
                        items[j]["payload"], measurements[items[j]["id"]],
                        15 * 60, 5000.0,
                    ):
                        union(i, j)

            components = {}
            for index, item in enumerate(items):
                components.setdefault(find(index), []).append(item)

            old_groups = {
                row["group_no"]: dict(row)
                for row in conn.execute("SELECT * FROM merge_groups WHERE status='pending'").fetchall()
            }
            target_to_members = {}
            for members in components.values():
                members.sort(key=lambda item: (item["payload"]["detected_at"], item["id"]))
                explicit = next((item.get("client_group_no") for item in members if item.get("client_group_no")), None)
                old_candidates = [item.get("merge_group_no") for item in members if item.get("merge_group_no")]
                component_region = members[0]["payload"]["region"]
                target = explicit
                if target is None:
                    target = self._retain_old_group(old_candidates, old_groups, members, component_region)
                if target is None:
                    target = uuid.uuid4().hex
                target_to_members[target] = (component_region, members)

            timestamp = now_iso()
            retained_groups = set()
            active_member_by_group = {}
            for group_no, (region, members) in target_to_members.items():
                if len(members) < 2:
                    continue
                retained_groups.add(group_no)
                active_member_by_group[group_no] = [item["id"] for item in members]
                anchor = members[0]["id"]
                existing = conn.execute("SELECT * FROM merge_groups WHERE group_no=?", (group_no,)).fetchone()
                group_changed = False
                members_changed = False
                new_group = existing is None
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO merge_groups(group_no,region,status,anchor_item_id,master_item_id,version,
                                                 created_at,updated_at)
                        VALUES(?,?, 'pending',?,NULL,1,?,?)
                        """,
                        (group_no, region, anchor, timestamp, timestamp),
                    )
                    group_changed = True
                else:
                    if existing["status"] != "pending":
                        continue
                    if existing["region"] != region or int(existing["anchor_item_id"] or 0) != int(anchor):
                        conn.execute(
                            "UPDATE merge_groups SET region=?, anchor_item_id=?, version=version+1, updated_at=? WHERE group_no=?",
                            (region, anchor, timestamp, group_no),
                        )
                        group_changed = True

                active_ids = {item["id"] for item in members}
                old_rows = conn.execute(
                    "SELECT * FROM merge_candidates WHERE group_no=? AND status='active'", (group_no,)
                ).fetchall()
                old_ids = {int(row["source_item_id"]) for row in old_rows}
                for item in members:
                    if item.get("merge_group_no") != group_no:
                        members_changed = True
                        conn.execute(
                            "UPDATE items SET merge_group_no=?, updated_at=? WHERE id=?",
                            (group_no, timestamp, item["id"]),
                        )
                        self.append_audit(conn, item["id"], "merge_group_recalculated", "system", "system",
                                          {"group_no": group_no, "reason": "candidate_recalculated"})
                    if item["id"] not in old_ids:
                        members_changed = True
                        conn.execute(
                            """
                            INSERT INTO merge_candidates(group_no,source_item_id,source_no,status,created_at,updated_at)
                            VALUES(?,?,?, 'active',?,?)
                            """,
                            (group_no, item["id"], item["source_no"], timestamp, timestamp),
                        )
                for row in old_rows:
                    if int(row["source_item_id"]) not in active_ids:
                        members_changed = True
                        conn.execute(
                            "UPDATE merge_candidates SET status='invalid', updated_at=? WHERE id=?",
                            (timestamp, row["id"]),
                        )
                        item = conn.execute("SELECT * FROM items WHERE id=?", (row["source_item_id"],)).fetchone()
                        if item:
                            self.append_audit(conn, row["source_item_id"], "merge_candidate_invalidated",
                                              "system", "system",
                                              {"group_no": group_no, "reason": "anchor_status_or_region_changed"})
                if not new_group and (group_changed or members_changed):
                    conn.execute(
                        "UPDATE merge_groups SET anchor_item_id=?, version=version+1, updated_at=? WHERE group_no=? AND status='pending'",
                        (anchor, timestamp, group_no),
                    )

            for group_no, group in old_groups.items():
                if group_no not in retained_groups:
                    conn.execute(
                        "UPDATE merge_groups SET status='invalidated', version=version+1, updated_at=? WHERE group_no=?",
                        (timestamp, group_no),
                    )
                    conn.execute(
                        "UPDATE merge_candidates SET status='invalid', updated_at=? WHERE group_no=? AND status='active'",
                        (timestamp, group_no),
                    )
                    active_in_group = active_member_by_group.get(group_no, [])
                    if active_in_group:
                        placeholders = ",".join("?" for _ in active_in_group)
                        stale_rows = conn.execute(
                            """
                            SELECT id FROM items
                            WHERE merge_group_no=? AND COALESCE(merge_status, 'source')='source'
                              AND id NOT IN (%s)
                            """ % placeholders,
                            tuple([group_no] + active_in_group),
                        ).fetchall()
                    else:
                        stale_rows = conn.execute(
                            "SELECT id FROM items WHERE merge_group_no=? AND COALESCE(merge_status, 'source')='source'",
                            (group_no,),
                        ).fetchall()
                    for stale in stale_rows:
                        conn.execute("UPDATE items SET merge_group_no=NULL, updated_at=? WHERE id=?",
                                     (timestamp, stale["id"]))
                        self.append_audit(conn, stale["id"], "merge_group_cleared", "system", "system",
                                          {"group_no": group_no, "reason": "less_than_two_candidates"})
                    anchor_id = group["anchor_item_id"]
                    if anchor_id:
                        self.append_audit(conn, anchor_id, "merge_group_invalidated", "system", "system",
                                          {"group_no": group_no, "reason": "no_valid_candidates"})
            groups = self.list_merge_groups("pending")
            conn.execute("COMMIT")
            return groups
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _retain_old_group(self, old_candidates, old_groups, members, region):
        member_ids = {item["id"] for item in members}
        for group_no in old_candidates:
            group = old_groups.get(group_no)
            if group and group["region"] == region and int(group["anchor_item_id"] or -1) in member_ids:
                return group_no
        for group_no in old_candidates:
            group = old_groups.get(group_no)
            if group and group["region"] == region:
                return group_no
        return None

    def designate_master(self, group_no, master_item_id, actor, role, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            group = conn.execute("SELECT * FROM merge_groups WHERE group_no=?", (group_no,)).fetchone()
            if group is None:
                raise NotFoundError("merge_group_not_found", "待归并组不存在")
            if group["status"] != "pending":
                raise ConflictError("merge_group_not_pending", "该归并组已由先写入的协调请求处理")
            if expected_version is not None and int(expected_version) != int(group["version"]):
                raise ConflictError("merge_version_conflict", "归并组选点已变化，请重新读取后再归并")

            candidates = conn.execute(
                "SELECT * FROM merge_candidates WHERE group_no=? AND status='active' ORDER BY id",
                (group_no,),
            ).fetchall()
            candidate_ids = [int(row["source_item_id"]) for row in candidates]
            if master_item_id not in candidate_ids:
                raise DomainError("master_not_candidate", "主事件必须来自当前待归并选点", 409)

            items = {}
            for item_id in candidate_ids:
                item_row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
                if item_row is None:
                    raise DomainError("merge_selection_stale", "待归并选点已失效，请重新计算", 409)
                item = self._row_to_item(item_row)
                if item["merge_status"] != "source" or item["status"] not in (
                    "pending", "assessed", "located", "suspended", "coordinating"
                ) or item["payload"].get("region") != group["region"]:
                    raise DomainError("merge_selection_stale", "待归并选点已失效，请重新计算", 409)
                items[item_id] = item

            timestamp = now_iso()
            origin_ids = [item_id for item_id in candidate_ids if item_id != master_item_id]
            transferred_sources = []
            superseded_sources = []
            superseded_by_origin = {}
            transferred_pending = []
            transferred_pending_by_origin = {}
            referenced_actions = []

            for origin_id in origin_ids:
                origin_transferred_sources = []
                origin_superseded_sources = []
                origin_transferred_pending = []
                source_rows = conn.execute(
                    "SELECT * FROM sources WHERE item_id=? AND origin_item_id=? AND status='active' ORDER BY id",
                    (origin_id, origin_id),
                ).fetchall()
                for source_row in source_rows:
                    duplicate = conn.execute(
                        """
                        SELECT s.id FROM sources s
                        WHERE s.item_id=? AND s.source_type=? AND s.external_id=? AND s.status='active'
                        """,
                        (master_item_id, source_row["source_type"], source_row["external_id"]),
                    ).fetchone()
                    if duplicate:
                        conn.execute("UPDATE sources SET status='superseded', group_no=?, updated_at=? WHERE id=?",
                                     (group_no, timestamp, source_row["id"]))
                        superseded_sources.append(source_row["id"])
                        origin_superseded_sources.append(source_row["id"])
                        continue
                    conn.execute("UPDATE sources SET status='transferred', group_no=? WHERE id=?",
                                 (group_no, source_row["id"]))
                    conn.execute(
                        """
                        INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at,
                                            status,origin_item_id,group_no)
                        VALUES(?,?,?,?,?,?, 'active',?,?)
                        """,
                        (master_item_id, source_row["source_type"], source_row["external_id"],
                         source_row["payload"], source_row["observed_at"], timestamp, origin_id, group_no),
                    )
                    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    transferred_sources.append({"from": source_row["id"], "to": new_id,
                                                "origin_item_id": origin_id})
                    origin_transferred_sources.append({"from": source_row["id"], "to": new_id,
                                                       "origin_item_id": origin_id})

                action_rows = conn.execute(
                    "SELECT * FROM actions WHERE item_id=? AND origin_item_id=? AND status='executed' ORDER BY id",
                    (origin_id, origin_id),
                ).fetchall()
                for action_row in action_rows:
                    exists = conn.execute(
                        "SELECT id FROM merge_action_refs WHERE group_no=? AND origin_action_id=?",
                        (group_no, action_row["id"]),
                    ).fetchone()
                    if not exists:
                        conn.execute(
                            """
                            INSERT INTO merge_action_refs(group_no,master_item_id,origin_item_id,origin_action_id,
                                                          action,payload,created_at)
                            VALUES(?,?,?,?,?,?,?)
                            """,
                            (group_no, master_item_id, origin_id, action_row["id"], action_row["action"],
                             action_row["payload"], timestamp),
                        )
                        referenced_actions.append(action_row["id"])

                pending_rows = conn.execute(
                    "SELECT * FROM pending_actions WHERE origin_item_id=? AND status IN ('queued', 'transferred')",
                    (origin_id,),
                ).fetchall()
                for pending_row in pending_rows:
                    conn.execute(
                        "UPDATE pending_actions SET item_id=?, group_no=?, status='transferred', updated_at=? WHERE id=?",
                        (master_item_id, group_no, timestamp, pending_row["id"]),
                    )
                    transferred_pending.append(pending_row["id"])
                    origin_transferred_pending.append(pending_row["id"])

                conn.execute(
                    """
                    UPDATE items SET merge_status='merged', merge_group_no=?, master_item_id=?, updated_at=?
                    WHERE id=?
                    """,
                    (group_no, master_item_id, timestamp, origin_id),
                )
                conn.execute(
                    "UPDATE merge_candidates SET status='transferred', source_no=?, updated_at=? WHERE group_no=? AND source_item_id=? AND status='active'",
                    (items[origin_id]["source_no"], timestamp, group_no, origin_id),
                )
                self.append_audit(conn, origin_id, "merged_into_master", actor, role, {
                    "group_no": group_no,
                    "master_item_id": master_item_id,
                    "master_source_no": items[master_item_id]["source_no"],
                    "source_no": items[origin_id]["source_no"],
                    "transferred_sources": origin_transferred_sources,
                    "superseded_sources": origin_superseded_sources,
                    "transferred_pending_actions": origin_transferred_pending,
                })

            conn.execute(
                """
                UPDATE items SET merge_status='master', merge_group_no=?, master_item_id=NULL, updated_at=?
                WHERE id=?
                """,
                (group_no, timestamp, master_item_id),
            )
            conn.execute(
                "UPDATE merge_candidates SET status='mastered', source_no=?, updated_at=? WHERE group_no=? AND source_item_id=? AND status='active'",
                (items[master_item_id]["source_no"], timestamp, group_no, master_item_id),
            )
            conn.execute(
                """
                UPDATE merge_groups
                SET status='mastered', master_item_id=?, version=version+1, updated_at=?
                WHERE group_no=?
                """,
                (master_item_id, timestamp, group_no),
            )
            self.append_audit(conn, master_item_id, "merge_master_designated", actor, role, {
                "group_no": group_no,
                "source_item_ids": candidate_ids,
                "origin_item_ids": origin_ids,
                "transferred_sources": transferred_sources,
                "superseded_sources": superseded_sources,
                "transferred_pending_actions": transferred_pending,
                "referenced_executed_actions": referenced_actions,
            })
            conn.execute("COMMIT")
            return self.get_merge_group(group_no)
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
            return {"counts": counts, "items": self.list_items(), "merge_groups": self.list_merge_groups()}
        finally:
            conn.close()
