#!/usr/bin/env python3
"""Tests for seed_catalogue.py. Run: python3 scripts/test_seed_catalogue.py

A fake process-path-management API (stdlib http.server, in-process) stands in for the service, so every
row of the seed's convergence table is exercised over real HTTP: create, no-op, revise, the immutable
and deactivated refusals, and --dry-run writing nothing. The last test loads the repository's real
catalogue/paths.json.
"""
import copy
import http.server
import io
import json
import os
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import seed_catalogue as seed  # noqa: E402

RELOCATE = {"pathId": "RELOCATE", "matchPrefix": "relocate", "direct": True, "requiredCapabilities": ["relocate"],
            "cycleTimeP95": "2h", "intake": ["SLOT_RELOCATION"]}
PICK = {"pathId": "PICK", "matchPrefix": "pick", "direct": True, "requiredCapabilities": ["pick"],
        "cycleTimeP95": "2h", "intake": ["CUSTOMER_ORDER"]}
DISPATCH = {"pathId": "DISPATCH", "matchPrefix": "dispatch", "direct": True, "requiredCapabilities": ["dispatch"],
            "cycleTimeP95": "2h", "intake": ["STOCK_TRANSFER"]}
REPLENISH = {"pathId": "REPLENISH", "matchPrefix": "replenish", "direct": True, "requiredCapabilities": ["replenish"],
             "cycleTimeP95": "2h", "intake": ["REPLENISHMENT"]}


def stored(desired, **over):
    """The GET shape the service returns for a defined path (Go duration String(), status)."""
    p = copy.deepcopy(desired)
    p["cycleTimeP95"] = "2h0m0s" if desired["cycleTimeP95"] == "2h" else desired["cycleTimeP95"]
    p["status"] = "ACTIVE"
    p.update(over)
    return p


class FakeApi(http.server.BaseHTTPRequestHandler):
    store, writes = {}, []

    def log_message(self, format, *args):  # noqa: A002 - silence the per-request log
        pass

    def _send(self, status, body):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _body(self):
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")

    def do_GET(self):
        pid = self.path.rsplit("/", 1)[-1]
        if pid in self.store:
            self._send(200, self.store[pid])
        else:
            self._send(404, {"type": "path-not-found"})

    def do_POST(self):
        body = self._body()
        FakeApi.writes.append(("POST", body["pathId"], self.headers.get("Idempotency-Key")))
        self.store[body["pathId"]] = stored(body)
        self._send(201, self.store[body["pathId"]])

    def do_PUT(self):
        pid, body = self.path.rsplit("/", 1)[-1], self._body()
        FakeApi.writes.append(("PUT", pid, sorted(body)))
        self.store[pid].update(body)
        self._send(200, self.store[pid])


class SeedCatalogue(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), FakeApi)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        FakeApi.store, FakeApi.writes = {}, []

    def run_seed(self, entries, *extra):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"paths": entries}, f)
        out = io.StringIO()
        with redirect_stdout(out):
            code = seed.main(["--base-url", self.base, "--file", f.name, *extra])
        os.unlink(f.name)
        return code, out.getvalue()

    def test_absent_entry_is_created_with_an_idempotency_key(self):
        code, out = self.run_seed([RELOCATE])
        self.assertEqual(code, 0, out)
        self.assertEqual(len(FakeApi.writes), 1, out)
        method, pid, key = FakeApi.writes[0]
        self.assertEqual((method, pid), ("POST", "RELOCATE"))
        self.assertTrue(key.startswith("seed-catalogue-RELOCATE-"), key)

    def test_identical_entry_writes_nothing_even_with_a_normalized_duration(self):
        FakeApi.store["RELOCATE"] = stored(RELOCATE)  # "2h" declared, "2h0m0s" stored
        code, out = self.run_seed([RELOCATE])
        self.assertEqual((code, FakeApi.writes), (0, []), out)
        self.assertIn("noop", out)

    def test_changed_revisable_field_is_revised_with_put(self):
        FakeApi.store["RELOCATE"] = stored(RELOCATE, cycleTimeP95="3h0m0s")
        code, out = self.run_seed([RELOCATE])
        self.assertEqual(code, 0, out)
        declared_revisable = sorted(k for k in seed.REVISABLE if k in RELOCATE)
        self.assertEqual(FakeApi.writes, [("PUT", "RELOCATE", declared_revisable)], out)
        self.assertEqual(FakeApi.store["RELOCATE"]["cycleTimeP95"], RELOCATE["cycleTimeP95"])

    def test_intake_drift_alone_is_revised_with_put(self):
        # A path defined before ADR 0034 stores intake [] (migration 0009's default).
        FakeApi.store["PICK"] = stored(PICK, intake=[])
        code, out = self.run_seed([PICK])
        self.assertEqual(code, 0, out)
        self.assertEqual([(m, pid) for m, pid, _ in FakeApi.writes], [("PUT", "PICK")], out)
        self.assertIn("intake", FakeApi.writes[0][2])
        self.assertEqual(FakeApi.store["PICK"]["intake"], ["CUSTOMER_ORDER"])

    def test_intake_from_an_older_server_without_the_field_is_revised(self):
        older = stored(PICK)
        del older["intake"]
        FakeApi.store["PICK"] = older
        code, out = self.run_seed([PICK])
        self.assertEqual(code, 0, out)
        self.assertEqual([(m, pid) for m, pid, _ in FakeApi.writes], [("PUT", "PICK")], out)

    def test_intake_is_compared_as_a_set(self):
        both = dict(RELOCATE, intake=["STOCK_TRANSFER", "SLOT_RELOCATION"])
        FakeApi.store["RELOCATE"] = stored(both, intake=["SLOT_RELOCATION", "STOCK_TRANSFER"])
        code, out = self.run_seed([both])
        self.assertEqual((code, FakeApi.writes), (0, []), out)

    def test_dropped_fake_eligibility_is_cleared_with_put(self):
        # RELOCATE as the first #160 draft defined it: a fake slot-relocation attribute, no intake.
        FakeApi.store["RELOCATE"] = stored(RELOCATE, intake=[],
                                           eligibility={"requiredProductAttributes": ["slot-relocation"]})
        code, out = self.run_seed([RELOCATE])
        self.assertEqual(code, 0, out)
        self.assertEqual([(m, pid) for m, pid, _ in FakeApi.writes], [("PUT", "RELOCATE")], out)
        self.assertNotIn("eligibility", FakeApi.writes[0][2])  # absent on PUT = the permissive default

    def test_an_entry_without_intake_is_rejected_at_load(self):
        no_intake = {k: v for k, v in RELOCATE.items() if k != "intake"}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"paths": [no_intake]}, f)
        try:
            with self.assertRaises(ValueError):
                seed.load(f.name)
        finally:
            os.unlink(f.name)

    def test_immutable_difference_is_refused_and_fails(self):
        FakeApi.store["RELOCATE"] = stored(RELOCATE, direct=False)
        code, out = self.run_seed([RELOCATE])
        self.assertEqual((code, FakeApi.writes), (1, []), out)
        self.assertIn("immutable", out)

    def test_deactivated_path_is_never_resurrected(self):
        FakeApi.store["RELOCATE"] = stored(RELOCATE, status="DEACTIVATED")
        code, out = self.run_seed([RELOCATE])
        self.assertEqual((code, FakeApi.writes), (1, []), out)
        self.assertIn("DEACTIVATED", out)

    def test_dry_run_writes_nothing(self):
        code, out = self.run_seed([RELOCATE], "--dry-run")
        self.assertEqual((code, FakeApi.writes), (0, []), out)
        self.assertIn("would create", out)

    def test_go_duration_parsing(self):
        self.assertEqual(seed.go_duration_seconds("2h"), seed.go_duration_seconds("2h0m0s"))
        self.assertEqual(seed.go_duration_seconds("1h30m0s"), seed.go_duration_seconds("90m"))
        self.assertIsNone(seed.go_duration_seconds("2 hours"))

    def test_repository_catalogue_declares_relocate(self):
        entries = seed.load(seed.DEFAULT_FILE)
        relocate = [e for e in entries if e["pathId"] == "RELOCATE"]
        self.assertEqual(len(relocate), 1)
        self.assertEqual(relocate[0]["requiredCapabilities"], ["relocate"])
        self.assertEqual(relocate[0]["matchPrefix"], "relocate")
        self.assertNotIn("eligibility", relocate[0])  # no fake slot-relocation attribute (ADR 0034)

    def test_repository_catalogue_declares_dispatch(self):
        # ADR 0035: the path id is the fulfillment-execution task type, so DISPATCH (not TRANSFER_DISPATCH).
        entries = seed.load(seed.DEFAULT_FILE)
        dispatch = [e for e in entries if e["pathId"] == "DISPATCH"]
        self.assertEqual(len(dispatch), 1)
        self.assertEqual(dispatch[0], DISPATCH)
        self.assertEqual([e["pathId"] for e in entries if e["pathId"].startswith("TRANSFER")], [])

    def test_repository_catalogue_declares_intake_for_every_path(self):
        intake = {e["pathId"]: e["intake"] for e in seed.load(seed.DEFAULT_FILE)}
        self.assertEqual(intake, {"PICK": ["CUSTOMER_ORDER"], "PACK": [], "REBIN": [], "SLAM": [],
                                  "RELOCATE": ["SLOT_RELOCATION"], "DISPATCH": ["STOCK_TRANSFER"],
                                  "REPLENISH": ["REPLENISHMENT"],
                                  # ADR 0038: the full-case and cross-dock flows
                                  "FULL_CASE": ["CUSTOMER_ORDER"], "CROSS_DOCK": ["CUSTOMER_ORDER", "STOCK_TRANSFER"],
                                  "BREAK_CASE": []})

    def test_repository_catalogue_declares_replenish(self):
        # ADR 0036: REPLENISH mirrors RELOCATE and is the only path that carries REPLENISHMENT.
        entries = seed.load(seed.DEFAULT_FILE)
        replenish = [e for e in entries if e["pathId"] == "REPLENISH"]
        self.assertEqual(len(replenish), 1)
        self.assertEqual(replenish[0], REPLENISH)
        self.assertNotIn("eligibility", replenish[0])
        carriers = [e["pathId"] for e in entries if "REPLENISHMENT" in e["intake"]]
        self.assertEqual(carriers, ["REPLENISH"])

    def test_repository_catalogue_declares_the_case_flows(self):
        # ADR 0038: FULL_CASE and CROSS_DOCK are gated by a required product attribute, so no existing
        # order line can be captured; BREAK_CASE is reached by an explicit path id only.
        by_id = {e["pathId"]: e for e in seed.load(seed.DEFAULT_FILE)}
        self.assertEqual(by_id["FULL_CASE"], {
            "pathId": "FULL_CASE", "matchPrefix": "full-case", "direct": True, "requiredCapabilities": ["pick"],
            "cycleTimeP95": "1h30m", "eligibility": {"requiredProductAttributes": ["fullCase"]},
            "intake": ["CUSTOMER_ORDER"]})
        self.assertEqual(by_id["CROSS_DOCK"], {
            "pathId": "CROSS_DOCK", "matchPrefix": "cross-dock", "direct": True, "requiredCapabilities": ["dispatch"],
            "destinationLocationRole": "Shipping", "cycleTimeP95": "1h",
            "eligibility": {"requiredProductAttributes": ["crossDock"]}, "intake": ["CUSTOMER_ORDER", "STOCK_TRANSFER"]})
        self.assertEqual(by_id["BREAK_CASE"], {
            "pathId": "BREAK_CASE", "matchPrefix": "break", "direct": True, "requiredCapabilities": ["break"],
            "cycleTimeP95": "2h", "intake": []})

    def test_case_flows_are_added_alone_to_an_environment_seeded_before_them(self):
        entries = seed.load(seed.DEFAULT_FILE)
        new = {"FULL_CASE", "CROSS_DOCK", "BREAK_CASE"}
        for e in entries:
            if e["pathId"] not in new:
                FakeApi.store[e["pathId"]] = stored(e)
        code, out = self.run_seed(entries)
        self.assertEqual(code, 0, out)
        self.assertEqual({(m, pid) for m, pid, _ in FakeApi.writes}, {("POST", pid) for pid in new}, out)
        self.assertEqual(FakeApi.store["CROSS_DOCK"]["destinationLocationRole"], "Shipping")
        FakeApi.writes.clear()
        code, out = self.run_seed(entries)  # re-run: the stored duration is the declared one, nothing drifts
        self.assertEqual((code, FakeApi.writes), (0, []), out)

    def test_case_flow_eligibility_drift_is_revised_with_put(self):
        entries = {e["pathId"]: e for e in seed.load(seed.DEFAULT_FILE)}
        FakeApi.store["FULL_CASE"] = stored(entries["FULL_CASE"], eligibility={})
        code, out = self.run_seed([entries["FULL_CASE"]])
        self.assertEqual(code, 0, out)
        self.assertEqual([(m, pid) for m, pid, _ in FakeApi.writes], [("PUT", "FULL_CASE")], out)
        self.assertEqual(FakeApi.store["FULL_CASE"]["eligibility"], {"requiredProductAttributes": ["fullCase"]})

    def test_a_different_destination_role_on_cross_dock_is_refused(self):
        cross = {e["pathId"]: e for e in seed.load(seed.DEFAULT_FILE)}["CROSS_DOCK"]
        FakeApi.store["CROSS_DOCK"] = stored(cross, destinationLocationRole="Drop")
        code, out = self.run_seed([cross])
        self.assertEqual((code, FakeApi.writes), (1, []), out)

    def test_replenish_is_created_once_then_converges_idempotently(self):
        # Seed the real catalogue twice against the fake API: REPLENISH is created once, the second
        # run writes nothing at all.
        entries = seed.load(seed.DEFAULT_FILE)
        code, out = self.run_seed(entries)
        self.assertEqual(code, 0, out)
        creates = [w for w in FakeApi.writes if w[0] == "POST" and w[1] == "REPLENISH"]
        self.assertEqual(len(creates), 1, out)
        self.assertTrue(creates[0][2].startswith("seed-catalogue-REPLENISH-"), creates[0][2])
        self.assertEqual(FakeApi.store["REPLENISH"]["intake"], ["REPLENISHMENT"])
        FakeApi.writes.clear()
        code, out = self.run_seed(entries)
        self.assertEqual((code, FakeApi.writes), (0, []), out)

    def test_replenish_alone_is_added_to_an_environment_seeded_before_it(self):
        # An environment seeded with the six earlier paths gets exactly one POST: nothing else drifts.
        entries = seed.load(seed.DEFAULT_FILE)
        for e in entries:
            if e["pathId"] != "REPLENISH":
                FakeApi.store[e["pathId"]] = stored(e)
        code, out = self.run_seed(entries)
        self.assertEqual(code, 0, out)
        self.assertEqual([(m, pid) for m, pid, _ in FakeApi.writes], [("POST", "REPLENISH")], out)

    def test_replenishment_intake_drift_is_revised_with_put(self):
        FakeApi.store["REPLENISH"] = stored(REPLENISH, intake=[])
        code, out = self.run_seed([REPLENISH])
        self.assertEqual(code, 0, out)
        self.assertEqual([(m, pid) for m, pid, _ in FakeApi.writes], [("PUT", "REPLENISH")], out)
        self.assertEqual(FakeApi.store["REPLENISH"]["intake"], ["REPLENISHMENT"])

    def test_dispatch_is_created_then_converges_idempotently_from_the_repository_catalogue(self):
        # Seed the real catalogue file twice against the fake API: DISPATCH is created once, and the
        # second run writes nothing at all (every declared entry is already present and identical).
        entries = seed.load(seed.DEFAULT_FILE)
        code, out = self.run_seed(entries)
        self.assertEqual(code, 0, out)
        creates = [w for w in FakeApi.writes if w[0] == "POST" and w[1] == "DISPATCH"]
        self.assertEqual(len(creates), 1, out)
        self.assertTrue(creates[0][2].startswith("seed-catalogue-DISPATCH-"), creates[0][2])
        self.assertEqual(FakeApi.store["DISPATCH"]["intake"], ["STOCK_TRANSFER"])
        FakeApi.writes.clear()
        code, out = self.run_seed(entries)
        self.assertEqual((code, FakeApi.writes), (0, []), out)


if __name__ == "__main__":
    unittest.main(verbosity=1)
