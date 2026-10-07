import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.domain import ConflictError, DomainError
from src.repository import Repository
from src.service import Service


class MergeLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def event(self, station, detected_at, longitude=120.0, region="north", frequency=2400.0):
        return self.service.create_item({
            "frequency_mhz": frequency,
            "bandwidth_mhz": 20.0,
            "station_id": station,
            "region": region,
            "strength_dbm": -40,
            "detected_at": detected_at,
            "reporter": "monitor-1",
            "latitude": 30.0,
            "longitude": longitude,
        }, "monitor-1", "monitor", region)

    def measurement(self, item, external_id, longitude=120.0):
        return self.service.add_source(item["id"], {
            "source_type": "scan",
            "external_id": external_id,
            "observed_at": "2026-10-07T10:01:00+00:00",
            "strength_dbm": -40,
            "latitude": 30.0,
            "longitude": longitude,
        }, "monitor-1", "monitor", "north")

    def test_similar_reports_enter_pending_group_and_merge_ledger(self):
        first = self.event("ST-01", "2026-10-07T10:00:00+00:00")
        second = self.event("ST-02", "2026-10-07T10:08:00+00:00", 120.01)
        first = self.service.get_item(first["id"])
        self.assertEqual(first["merge_group_no"], second["merge_group_no"])
        group = self.repo.get_merge_group(first["merge_group_no"])
        self.assertEqual(group["status"], "pending")
        self.assertEqual({c["source_item_id"] for c in group["candidates"]}, {first["id"], second["id"]})

        self.measurement(first, "M-1")
        self.measurement(second, "M-2")
        second = self.service.act(second["id"], "assess", {}, "analyst-1", "analyst", second["version"])
        pending = self.service.queue_action(second["id"], "locate", {
            "location": "cell-7",
            "confidence": 0.9,
            "client_action_id": "LOC-1",
        }, "field-1", "field_operator", "north")

        version = self.repo.get_merge_group(first["merge_group_no"])["version"]
        self.service.designate_master(
            first["merge_group_no"], {"master_item_id": first["id"]},
            "coord-1", "coordinator", "north", version
        )

        master = self.service.get_item(first["id"])
        origin = self.service.get_item(second["id"])
        self.assertEqual(master["merge_status"], "master")
        self.assertEqual(origin["merge_status"], "merged")
        self.assertEqual(origin["master_item_id"], master["id"])
        self.assertIsNotNone(origin["source_no"])

        source_origins = sorted(source["origin_item_id"] for source in master["sources"])
        self.assertEqual(source_origins, [first["id"], second["id"]])
        self.assertEqual([ref["origin_item_id"] for ref in master["merge_action_refs"]], [second["id"]])
        self.assertEqual(master["actions"], [])

        transferred = next(action for action in master["pending_actions"] if action["id"] == pending["id"])
        self.assertEqual(transferred["item_id"], master["id"])
        self.assertEqual(transferred["origin_item_id"], second["id"])
        self.assertEqual(transferred["status"], "transferred")

        master = self.service.act(master["id"], "assess", {}, "analyst-1", "analyst", master["version"])
        executed = self.service.execute_pending_action(
            pending["id"], {}, "field-1", "field_operator", master["version"], "north"
        )
        self.assertEqual(executed["status"], "located")
        with self.assertRaises(ConflictError):
            self.service.execute_pending_action(
                pending["id"], {}, "field-1", "field_operator", executed["version"], "north"
            )

    def test_group_version_rejects_late_selection_and_cross_region_coordinator(self):
        first = self.event("ST-03", "2026-10-07T11:00:00+00:00")
        second = self.event("ST-04", "2026-10-07T11:05:00+00:00", 120.01)
        first = self.service.get_item(first["id"])
        group_no = first["merge_group_no"]
        version = self.repo.get_merge_group(group_no)["version"]
        with self.assertRaises(DomainError) as forbidden:
            self.service.designate_master(
                group_no, {"master_item_id": first["id"]},
                "coord-x", "coordinator", "south", version
            )
        self.assertEqual(forbidden.exception.code, "region_mismatch")
        self.service.designate_master(
            group_no, {"master_item_id": second["id"]},
            "coord-1", "coordinator", "north", version
        )
        with self.assertRaises(ConflictError) as conflict:
            self.service.designate_master(
                group_no, {"master_item_id": first["id"]},
                "coord-2", "coordinator", "north", version
            )
        self.assertEqual(conflict.exception.code, "merge_group_not_pending")

    def test_status_and_region_change_recalculate_candidates(self):
        first = self.event("ST-05", "2026-10-07T12:00:00+00:00")
        second = self.event("ST-06", "2026-10-07T12:07:00+00:00", 120.01)
        first = self.service.get_item(first["id"])
        second = self.service.get_item(second["id"])
        group_no = first["merge_group_no"]
        self.service.act(
            first["id"], "change_region", {"new_region": "south"},
            "coord-1", "coordinator", first["version"], "north"
        )
        first = self.service.get_item(first["id"])
        second = self.service.get_item(second["id"])
        old_group = self.repo.get_merge_group(group_no)
        old_candidates = {c["source_item_id"]: c["status"] for c in old_group["candidates"]}
        self.assertEqual(old_candidates[first["id"]], "invalid")
        self.assertEqual(old_candidates[second["id"]], "invalid")
        self.assertIsNone(first["merge_group_no"])
        self.assertIsNone(second["merge_group_no"])

    def test_offline_sync_merges_by_group_and_deduplicates_actions(self):
        payload = {
            "group_no": "offline-group-1",
            "reports": [
                {"client_ref": "r1", "payload": {
                    "frequency_mhz": 2400.0, "bandwidth_mhz": 20.0, "station_id": "ST-07",
                    "region": "north", "strength_dbm": -40,
                    "detected_at": "2026-10-07T13:00:00+00:00", "reporter": "m",
                    "latitude": 30.0, "longitude": 120.0,
                }},
                {"client_ref": "r2", "payload": {
                    "frequency_mhz": 2400.0, "bandwidth_mhz": 20.0, "station_id": "ST-08",
                    "region": "north", "strength_dbm": -40,
                    "detected_at": "2026-10-07T13:07:00+00:00", "reporter": "m",
                    "latitude": 30.0, "longitude": 120.01,
                }},
            ],
            "measurements": [
                {"report_ref": "r1", "source_type": "scan", "external_id": "OFF-1",
                 "observed_at": "2026-10-07T13:01:00+00:00", "strength_dbm": -40,
                 "latitude": 30.0, "longitude": 120.0},
                {"report_ref": "r2", "source_type": "scan", "external_id": "OFF-2",
                 "observed_at": "2026-10-07T13:01:00+00:00", "strength_dbm": -40,
                 "latitude": 30.0, "longitude": 120.0},
            ],
            "actions": [
                {"report_ref": "r1", "action": "assess", "client_action_id": "OFF-A1",
                 "actor": "analyst-1", "role": "analyst", "payload": {}},
                {"report_ref": "r2", "action": "assess", "client_action_id": "OFF-A2",
                 "actor": "analyst-1", "role": "analyst", "payload": {}},
            ],
        }
        first = self.service.sync_group(payload, "monitor-1", "monitor", "north")
        self.assertEqual(len(first["items"]), 2)
        self.assertEqual(len(first["actions"]), 2)
        repeated = self.service.sync_group(payload, "monitor-1", "monitor", "north")
        self.assertEqual(len(repeated["actions"]), 2)
        self.assertTrue(all(action["duplicated"] for action in repeated["actions"]))
        self.assertEqual(len(self.repo.list_pending_actions(status="queued")), 2)
        self.assertEqual({item["merge_group_no"] for item in repeated["items"]}, {"offline-group-1"})


if __name__ == "__main__":
    unittest.main()
