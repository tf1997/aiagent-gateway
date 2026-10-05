#!/usr/bin/env python3
"""Hermes resource ownership, Runs, Responses and two-instance drain/failure checks."""
from datetime import datetime, timezone
import json
import subprocess
import time
import uuid
from gateway import ADMIN_URL, DATA_URL, STATE, api, credentials, require
from manage import write_private
from check_gateway import expect, stream

BASE = DATA_URL + "/agents/hermes-local"
TERMINAL = {"completed", "failed", "cancelled", "interrupted"}


def main():
    creds = credentials()
    token, other = (c["token"] for c in creds["callers"])
    admin = creds["admin_token"]
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "passed": False, "checks": {}}
    path = STATE / "integration-native.json"
    def record(name, value):
        report["checks"][name] = value
        write_private(path, json.dumps(report, indent=2) + "\n")
        print("PASS " + name, flush=True)
    def route():
        return next(r for r in require(ADMIN_URL + "/admin/v1/routes", admin)["items"] if r["key"] == "hermes-local")
    def endpoint_state(endpoint_id, state):
        current = route()
        ep = next(e for e in current["endpoints"] if e["id"] == endpoint_id)
        return require(ADMIN_URL + "/admin/v1/routes/" + current["id"] + "/endpoints/" + endpoint_id,
                       admin, {"state": state}, "PATCH", {"If-Match": '"' + str(ep["version"]) + '"'})
    def resource_rows():
        return require(ADMIN_URL + "/admin/v1/routes/" + route()["id"] + "/resources?resource_kind=hermes_session", admin)["items"]
    def create(alias=None):
        return require(BASE + "/api/sessions", token, {"id": alias or "native-" + uuid.uuid4().hex})["session"]["id"]
    write_private(path, json.dumps(report, indent=2) + "\n")
    stopped = False
    draining = None
    try:
        expect(route()["agent_type"] == "hermes" and len(route()["endpoints"]) == 2, "Need a Hermes route with two instances")
        shared_alias = "shared-" + uuid.uuid4().hex
        first = create(shared_alias)
        second = create()
        foreign = require(BASE + "/api/sessions", other, {"id": shared_alias})["session"]["id"]
        expect(first != foreign, "Callers share a public session")
        expect(api(BASE + "/api/sessions/" + first, other)[0] == 404, "Foreign caller accessed session")
        rows = {row["public_id"]: row for row in resource_rows()}
        expect(rows[first]["endpoint_id"] != rows[second]["endpoint_id"], "New sessions did not use both instances")
        expect(api(BASE + "/api/sessions/" + rows[first]["upstream_id"], token)[0] == 404, "Raw upstream session is publicly accessible")
        expect(api(BASE + "/api/sessions/" + first + "/chat", token, {"input": "invalid", "session_id": second})[0] == 409, "Conflicting session was accepted")
        listing = require(BASE + "/api/sessions?limit=1", token)
        expect(listing["next_page_token"], "Session pagination did not return a cursor")
        expect(api(BASE + "/api/sessions?limit=1&page_token=" + listing["next_page_token"], other)[0] == 400, "Foreign caller reused session cursor")
        record("ownership_and_two_instances", {"session_ids": [first, second], "endpoint_ids": sorted({rows[first]["endpoint_id"], rows[second]["endpoint_id"]})})
        print("Checking Runs...", flush=True)
        run_body = {"session_id": first, "input": "只回复 NATIVE_RUN_READY。不要调用工具。"}
        idem = {"Idempotency-Key": "native-" + uuid.uuid4().hex}
        run = require(BASE + "/v1/runs", token, run_body, headers=idem)
        replay = require(BASE + "/v1/runs", token, run_body, headers=idem)
        expect(run["run_id"] == replay["run_id"], "Idempotency changed run")
        expect(api(BASE + "/v1/runs", token, {**run_body, "input": "different"}, headers=idem)[0] == 409, "Changed idempotent input accepted")
        run_path = BASE + "/v1/runs/" + run["run_id"]
        event_ids = []
        frames, timing = stream(run_path + "/events", token, event_ids=event_ids)
        events = [v for _, v in frames if isinstance(v, dict)]
        expect(any(v.get("event") == "run.completed" for v in events), "Run SSE did not complete")
        expect(all(v.get("run_id", run["run_id"]) == run["run_id"] for v in events), "Run event identity changed")
        finished = require(run_path, token)
        expect(finished["status"] == "completed" and finished["session_id"] == first, "Run status/session mismatch")
        seqs = [int(value) for value in event_ids if value.isdigit()]
        replay_check = {"supported": bool(seqs)}
        if seqs:
            replay_frames, _ = stream(run_path + "/events", token, headers={"Last-Event-ID": str(max(seqs) - 1)})
            replay_events = [v for _, v in replay_frames if isinstance(v, dict)]
            expect(any(v.get("event") == "run.completed" for v in replay_events), "Last-Event-ID replay missed completion")
            replay_check["last_event_id"] = max(seqs) - 1
        else:
            # v2026.9.24 has a destructive queue; completed subscriptions are dropped.
            status = api(run_path + "/events", token, headers={"Last-Event-ID": "0"})[0]
            expect(status == 404, "Unexpected one-shot event queue behavior")
            replay_check.update(upstream_version="v2026.9.24", completed_subscription_status=status)
        expect(api(run_path, other)[0] == 404, "Foreign caller queried Run")
        record("runs_and_replay_boundary", {"run_id": run["run_id"], "idempotency": True, "replay": replay_check, "sse": timing})
        print("Checking steer/stop...", flush=True)
        pending = require(BASE + "/v1/runs", token, {"session_id": second, "input": "写一段关于春天的简短描述，不要调用工具。"})
        control = BASE + "/v1/runs/" + pending["run_id"]
        for _ in range(30):
            steer = api(control + "/steer", token, {"input": "请简短回复。"})
            if steer[0] != 409:
                break
            time.sleep(0.1)
        expect(steer[0] == 200, "Active Run steer failed")
        stop = require(control + "/stop", token, {})
        expect(stop["run_id"] == pending["run_id"], "Stop changed Run identity")
        for _ in range(90):
            status = require(control, token)
            if status["status"] in TERMINAL:
                break
            time.sleep(1)
        expect(status["status"] in TERMINAL, "Stopped Run did not settle")
        record("run_controls", {"run_id": pending["run_id"], "steer": True, "final_status": status["status"]})
        print("Checking Responses...", flush=True)
        marker = "yellow-pear-" + uuid.uuid4().hex[:8]
        response = require(BASE + "/v1/responses", token, {"model": "hermes-agent", "input": "公开虚构标签是 " + marker + "，请记住，只回复 SAVED；不要调用工具。"})
        frames, timing = stream(BASE + "/v1/responses", token, {"model": "hermes-agent", "previous_response_id": response["id"], "input": "上一条用户消息中的公开虚构标签是什么？只回复标签，不要调用工具。", "stream": True})
        complete = [v["response"] for name, v in frames if name == "response.completed" and isinstance(v, dict)]
        expect(bool(complete) and marker in json.dumps(complete, ensure_ascii=False), "Responses chain did not recall label")
        expect(api(BASE + "/v1/responses", other, {"previous_response_id": response["id"], "input": "invalid"})[0] == 404, "Foreign caller reused response chain")
        record("response_chain", {"response_id": response["id"], "recall": True, "sse": timing})
        current = route()
        draining = rows[first]["endpoint_id"]
        endpoint_state(draining, "draining")
        require(BASE + "/api/sessions/" + first, token)
        fresh = create()
        fresh_row = next(row for row in resource_rows() if row["public_id"] == fresh)
        expect(fresh_row["endpoint_id"] != draining, "Draining instance received a new session")
        expect(api(BASE + "/api/sessions/" + first + "/fork", token, {})[0] == 503, "Draining instance accepted fork")
        current_ep = next(e for e in route()["endpoints"] if e["id"] == draining)
        status, _, _ = api(ADMIN_URL + "/admin/v1/routes/" + current["id"] + "/endpoints/" + draining, admin,
                           method="DELETE", headers={"If-Match": '"' + str(current_ep["version"]) + '"'})
        expect(status == 409, "Referenced endpoint was deletable")
        endpoint_state(draining, "active")
        draining = None
        record("drain_and_reference_protection", True)
        replica = next(e for e in route()["endpoints"] if e["interfaces"]["proxy"]["url"].endswith(":18643"))
        on_replica = next(sid for sid in (first, second) if rows[sid]["endpoint_id"] == replica["id"])
        stopped = True
        subprocess.run(["docker", "stop", "aiagent-hermes-local-hermes2-1"], check=True, stdout=subprocess.DEVNULL)
        detail_url = ADMIN_URL + "/admin/v1/routes/" + current["id"] + "/endpoints/" + replica["id"]
        for _ in range(45):
            detail = require(detail_url, admin)
            if not detail["runtime"]["interfaces"]["proxy"]["healthy"]:
                break
            time.sleep(1)
        expect(not detail["runtime"]["interfaces"]["proxy"]["healthy"], "Stopped replica remained healthy")
        expect(api(BASE + "/api/sessions/" + on_replica, token)[0] == 503, "Existing session moved after failure")
        fresh = create()
        fresh_row = next(row for row in resource_rows() if row["public_id"] == fresh)
        expect(fresh_row["endpoint_id"] != replica["id"], "New session selected failed instance")
        record("failed_instance_does_not_move_sessions", True)
        subprocess.run(["docker", "start", "aiagent-hermes-local-hermes2-1"], check=True, stdout=subprocess.DEVNULL)
        stopped = False
        for _ in range(60):
            detail = require(detail_url, admin)
            if detail["runtime"]["interfaces"]["proxy"]["ready"]:
                break
            time.sleep(1)
        expect(detail["runtime"]["interfaces"]["proxy"]["ready"], "Replica did not recover readiness")
        require(BASE + "/api/sessions/" + on_replica, token)
        record("replica_recovered_on_original_session", True)
        report["passed"] = True
    except Exception as exc:
        report["error_type"] = type(exc).__name__
        if isinstance(exc, RuntimeError):
            report["error"] = str(exc)
        raise
    finally:
        if draining:
            endpoint_state(draining, "active")
        if stopped:
            subprocess.run(["docker", "start", "aiagent-hermes-local-hermes2-1"], check=True, stdout=subprocess.DEVNULL)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_private(path, json.dumps(report, indent=2) + "\n")
    print("通过：Hermes 原生资源与双实例检查。", flush=True)


if __name__ == "__main__":
    main()
