import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


def payload(station="ST-01", region="north", freq=2400.0, at="2026-09-27T10:00:00+00:00", strength=-55):
    return {
        "frequency_mhz": freq,
        "bandwidth_mhz": 10.0,
        "station_id": station,
        "region": region,
        "strength_dbm": strength,
        "detected_at": at,
        "reporter": "monitor-1",
    }


class MergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_duplicate_reports_enter_pending_area(self):
        a = self.service.create_item(payload(at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(at="2026-09-27T10:10:00+00:00"), "m", "monitor")
        groups = self.service.list_merge_groups("pending")
        self.assertEqual(len(groups), 1)
        self.assertEqual({m["item_id"] for m in groups[0]["members"]}, {a["id"], b["id"]})

    def test_different_region_or_time_or_freq_is_not_grouped(self):
        self.service.create_item(payload(region="north", at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        self.service.create_item(payload(region="south", at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        self.service.create_item(payload(region="north", at="2026-09-27T10:40:00+00:00"), "m", "monitor")
        self.service.create_item(payload(region="north", freq=2500.0, at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        groups = self.service.list_merge_groups("pending")
        self.assertEqual(len(groups), 4)
        for group in groups:
            self.assertEqual(len(group["members"]), 1)

    def test_merge_transfers_measurements_and_planned_actions(self):
        a = self.service.create_item(payload(at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        group = self.service.list_merge_groups("pending")[0]

        self.service.add_source(
            b["id"],
            {"source_type": "station_measurement", "external_id": "SRC-B-1",
             "observed_at": "2026-09-27T10:06:00+00:00", "strength_dbm": -60,
             "station_id": "ST-01", "frequency_mhz": 2400.0},
            "m", "monitor",
        )
        self.service.create_planned_action(
            b["id"], {"action": "suspend", "payload": {"authorization_code": "REG-X"}},
            "c", "coordinator", "north",
        )

        merged = self.service.merge_group(group["id"], a["id"], "c", "coordinator", "north", group["version"])
        self.assertEqual(merged["status"], "merged")
        self.assertEqual(merged["main_item_id"], a["id"])

        main = self.service.get_item(a["id"])
        member = self.service.get_item(b["id"])

        # 测量记录整批转过去，来源编号保留
        self.assertTrue(any(s["external_id"] == "SRC-B-1" for s in main["sources"]))
        # 未执行（拟办）动作整批转过去
        self.assertTrue(
            any(p["action"] == "suspend" and p["status"] == "transferred" for p in main["planned_actions"])
        )
        # 原事件标成已归并
        self.assertEqual(member["status"], "merged")
        self.assertEqual(member["payload"]["merged_into"], a["id"])
        # 已执行动作只留记录，不再重放：主事件 actions 表不出现 b 的动作行
        self.assertEqual(main["actions"], [])

    def test_executed_actions_are_not_replayed_on_main(self):
        a = self.service.create_item(payload(at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        group = self.service.list_merge_groups("pending")[0]

        b = self.service.act(b["id"], "assess", {}, "m", "monitor", b["version"])
        b_action_count = len(b["actions"])

        self.service.merge_group(group["id"], a["id"], "c", "coordinator", "north", group["version"])
        main = self.service.get_item(a["id"])
        # b 的已执行动作仍留在 b 的记录里
        member = self.service.get_item(b["id"])
        self.assertEqual(len(member["actions"]), b_action_count)
        # 主事件不重放 b 的 assess
        self.assertNotIn("assess", {act["action"] for act in main["actions"]})

    def test_main_status_change_invalidates_and_recalculates(self):
        a = self.service.create_item(payload(at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        group = self.service.list_merge_groups("pending")[0]
        self.service.create_planned_action(
            b["id"], {"action": "suspend", "payload": {"authorization_code": "REG-X"}},
            "c", "coordinator", "north",
        )
        self.service.merge_group(group["id"], a["id"], "c", "coordinator", "north", group["version"])
        main = self.service.get_item(a["id"])
        self.assertEqual(main["planned_actions"][0]["status"], "transferred")

        # 主事件状态变更 → 归并组失效重算，转办动作退回待办
        a = self.service.act(a["id"], "assess", {}, "m", "monitor", a["version"])
        refetched = self.service.get_merge_group(group["id"])
        self.assertEqual(refetched["status"], "invalidated")
        main = self.service.get_item(a["id"])
        self.assertEqual(main["planned_actions"][0]["status"], "planned")

        pending = self.service.list_merge_groups("pending")
        self.assertTrue(any(a["id"] in {m["item_id"] for m in g["members"]} for g in pending))

    def test_concurrent_merge_keeps_first_write(self):
        a = self.service.create_item(payload(at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        group = self.service.list_merge_groups("pending")[0]

        first = self.service.merge_group(group["id"], a["id"], "c1", "coordinator", "north", group["version"])
        self.assertEqual(first["status"], "merged")
        with self.assertRaises(ConflictError) as ctx:
            self.service.merge_group(group["id"], a["id"], "c2", "coordinator", "north", group["version"])
        self.assertEqual(ctx.exception.code, "version_conflict")

    def test_cannot_merge_other_jurisdiction(self):
        a = self.service.create_item(payload(region="north", at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(region="north", at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        group = self.service.list_merge_groups("pending")[0]

        with self.assertRaises(DomainError) as ctx:
            self.service.merge_group(group["id"], a["id"], "c", "coordinator", "south", group["version"])
        self.assertEqual(ctx.exception.code, "region_mismatch")
        self.assertEqual(ctx.exception.status, 403)

    def test_only_coordinator_can_merge(self):
        a = self.service.create_item(payload(at="2026-09-27T10:00:00+00:00"), "m", "monitor")
        b = self.service.create_item(payload(at="2026-09-27T10:05:00+00:00"), "m", "monitor")
        group = self.service.list_merge_groups("pending")[0]
        with self.assertRaises(DomainError) as ctx:
            self.service.merge_group(group["id"], a["id"], "m", "monitor", "north", group["version"])
        self.assertEqual(ctx.exception.code, "forbidden")

    def test_offline_sync_by_group_no_is_idempotent(self):
        item_a = payload(at="2026-09-27T10:00:00+00:00")
        item_b = payload(at="2026-09-27T10:05:00+00:00")
        batch = {
            "group_no": "MG-001",
            "items": [item_a, item_b],
            "actions": [
                {"stable_key": self._stable_key(item_a), "action": "assess",
                 "payload": {}, "action_key": "act-1"},
            ],
        }
        first = self.service.sync_batch(batch, "m", "monitor", "north")
        self.assertEqual(first["items_created"], 2)
        self.assertEqual(first["actions_created"], 1)

        # 断网重连后重复回网：按组号合并，重复不新增动作
        second = self.service.sync_batch(batch, "m", "monitor", "north")
        self.assertEqual(second["items_created"], 0)
        self.assertEqual(second["items_duplicated"], 2)
        self.assertEqual(second["actions_created"], 0)
        self.assertEqual(second["actions_duplicated"], 1)

        # 事件仍进入同一个待归并组
        groups = self.service.list_merge_groups("pending")
        self.assertEqual(len(groups), 1)

    def _stable_key(self, p):
        return "%s|%s|%s|%s" % (p["station_id"], p["region"], p["frequency_mhz"], p["detected_at"])


if __name__ == "__main__":
    unittest.main()
