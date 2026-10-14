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
            "cycleTimeP95": "2h", "eligibility": {"requiredProductAttributes": ["slot-relocation"]}}


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
        FakeApi.store["RELOCATE"] = stored(RELOCATE, eligibility={})
        code, out = self.run_seed([RELOCATE])
        self.assertEqual(code, 0, out)
        self.assertEqual(FakeApi.writes, [("PUT", "RELOCATE", sorted(seed.REVISABLE))], out)
        self.assertEqual(FakeApi.store["RELOCATE"]["eligibility"], RELOCATE["eligibility"])

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


if __name__ == "__main__":
    unittest.main(verbosity=1)
