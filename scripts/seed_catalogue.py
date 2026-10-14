#!/usr/bin/env python3
"""seed_catalogue: apply the declared process-path catalogue through the public REST API (ADR 0033, 0034).

catalogue/paths.json is the reviewed record of the paths every environment must define. Each entry is a
POST /process-paths body. This script converges a running process-path-management to it through the
same use case, outbox and topic an operator would use, so every catalogue consumer (wes-work-planning,
fulfillment-execution, workforce-management, order-management) receives the change from
warehouse.process-path-management.events. It is idempotent and safe to re-run:

  store state vs declaration                       action
  absent                                           POST /process-paths            -> ProcessPathCreated
  present, identical                               nothing (no spurious ProcessPathUpdated)
  present, a revisable field differs               PUT /process-paths/{pathId}    -> ProcessPathUpdated
    (matchPrefix, requiredCapabilities, cycleTimeP95, eligibility, intake; intake drift alone, e.g. a path
     defined before ADR 0034 that stores intake [], is revised like any other field)
  present, direct/destinationLocationRole differ   reported, exit 1 (immutable: define a new pathId)
  present, DEACTIVATED                             reported, exit 1 (a deactivated id is never resurrected)

Usage:
  kubectl port-forward -n warehouse-systems svc/process-path-management 8080:80
  python3 scripts/seed_catalogue.py --base-url http://localhost:8080 --dry-run
  python3 scripts/seed_catalogue.py --base-url http://localhost:8080

Exit 0 when every entry is converged (or would be, with --dry-run), 1 otherwise. Stdlib only.
"""
import argparse
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request

DEFAULT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "catalogue", "paths.json")
REVISABLE = ("matchPrefix", "requiredCapabilities", "cycleTimeP95", "eligibility", "intake")
_DURATION_UNITS = {"ns": 1e-9, "us": 1e-6, "\u00b5s": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}
_DURATION_RX = re.compile(r"(\d+(?:\.\d+)?)(ns|us|\u00b5s|ms|s|m|h)")


def go_duration_seconds(text):
    """Seconds in a Go duration string ("2h", "2h0m0s", "90m"); None when it is not one."""
    text = (text or "").strip()
    if not text:
        return None
    pos, total = 0, 0.0
    for m in _DURATION_RX.finditer(text):
        if m.start() != pos:
            return None
        total += float(m.group(1)) * _DURATION_UNITS[m.group(2)]
        pos = m.end()
    return total if pos == len(text) else None


def normalize_eligibility(e):
    e = e or {}
    return (e.get("maxUnitsPerLine"), tuple(e.get("requiredProductAttributes") or ()),
            tuple(e.get("excludedProductAttributes") or ()), bool(e.get("nonSortable")))


def normalize_intake(values):
    """The intake is a set (ADR 0034): order and absence do not matter, [] and a missing field are equal."""
    return tuple(sorted(values or ()))


def revisable_view(p):
    return (p.get("matchPrefix"), tuple(p.get("requiredCapabilities") or ()),
            go_duration_seconds(p.get("cycleTimeP95")), normalize_eligibility(p.get("eligibility")),
            normalize_intake(p.get("intake")))


def immutable_view(p):
    return bool(p.get("direct")), p.get("destinationLocationRole") or ""


def plan(desired, stored):
    """-> (action, detail). action is one of create, noop, revise, error."""
    if stored is None:
        return "create", "absent"
    if stored.get("status") == "DEACTIVATED":
        return "error", "is DEACTIVATED: a deactivated id is never resurrected, declare a new pathId"
    if immutable_view(stored) != immutable_view(desired):
        return "error", (f"immutable fields differ (stored direct/destinationLocationRole={immutable_view(stored)}, "
                         f"declared {immutable_view(desired)}): declare a new pathId")
    if revisable_view(stored) == revisable_view(desired):
        return "noop", "identical"
    return "revise", f"stored {revisable_view(stored)} -> declared {revisable_view(desired)}"


class Api:
    def __init__(self, base_url, timeout=10):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def call(self, method, path, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw) if raw else None
            except ValueError:
                return e.code, raw.decode(errors="replace")

    def get_path(self, path_id):
        status, body = self.call("GET", "/process-paths/" + path_id)
        if status == 404:
            return None
        if status != 200:
            raise RuntimeError(f"GET /process-paths/{path_id} -> {status} {body}")
        return body


def load(path):
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    entries = doc.get("paths") or []
    for e in entries:
        missing = [k for k in ("pathId", "matchPrefix", "requiredCapabilities", "cycleTimeP95") if not e.get(k)]
        if missing:
            raise ValueError(f"{path}: entry {e.get('pathId')!r} is missing {missing}")
        if not isinstance(e.get("intake"), list):
            # ADR 0034: every entry declares its demand sources, [] included; an omitted intake would
            # leave the routing decision to whatever the store happens to hold.
            raise ValueError(f"{path}: entry {e.get('pathId')!r} must declare an intake array (ADR 0034)")
    return entries


def apply_entry(api, desired, dry_run):
    """-> (ok, line)"""
    pid = desired["pathId"]
    action, detail = plan(desired, api.get_path(pid))
    if action in ("noop", "error") or dry_run:
        return action != "error", f"{pid}: {'would ' if dry_run and action in ('create', 'revise') else ''}{action} ({detail})"
    if action == "create":
        key = "seed-catalogue-" + pid + "-" + hashlib.sha256(json.dumps(desired, sort_keys=True).encode()).hexdigest()[:16]
        status, body = api.call("POST", "/process-paths", desired, {"Idempotency-Key": key})
        ok = status == 201
    else:
        status, body = api.call("PUT", "/process-paths/" + pid, {k: desired[k] for k in REVISABLE if k in desired})
        ok = status == 200
    return ok, f"{pid}: {action} -> {status}" + ("" if ok else f" {body}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Apply catalogue/paths.json through the process-path-management REST API.")
    ap.add_argument("--base-url", required=True, help="process-path-management base URL, e.g. http://localhost:8080")
    ap.add_argument("--file", default=DEFAULT_FILE, help="declared catalogue (default: catalogue/paths.json)")
    ap.add_argument("--dry-run", action="store_true", help="report what would change, write nothing")
    a = ap.parse_args(argv)
    api, failures = Api(a.base_url), 0
    for desired in load(a.file):
        try:
            ok, line = apply_entry(api, desired, a.dry_run)
        except (RuntimeError, OSError) as e:
            ok, line = False, f"{desired['pathId']}: {e}"
        failures += 0 if ok else 1
        print(("ok    " if ok else "ERROR ") + line)
    print(f"seed-catalogue: {failures} entr{'y' if failures == 1 else 'ies'} not converged")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
