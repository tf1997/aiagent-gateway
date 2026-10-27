#!/usr/bin/env python3
"""Real HTTP/SSE and A2A interop through the local gateway; uses synthetic conversations."""
import argparse
from datetime import datetime, timezone
import json
import sys
import time
import urllib.parse
import urllib.request
import uuid

from gateway import ADMIN_URL, DATA_URL, STATE, HTTP, credentials, load, require
from manage import write_private

MAX_EVENT = 2 << 20
MAX_RESPONSE = 8 << 20


class CheckFailure(RuntimeError):
    """A fixed assertion message with optional diagnostics that exclude model text."""
    def __init__(self, message, diagnostics=None):
        super().__init__(message)
        self.diagnostics = diagnostics or {}


def expect(condition, message, diagnostics=None):
    if not condition:
        raise CheckFailure(message, diagnostics)


def events(response, with_ids=False):
    """Parse LF/CRLF SSE from HTTP responses with bounded frames and total bytes."""
    data, event = [], "message"
    event_id = ""
    size = total = 0
    for raw in iter(lambda: response.readline(MAX_EVENT + 1), b""):
        total += len(raw)
        size += len(raw)
        expect(len(raw) <= MAX_EVENT and size <= MAX_EVENT and total <= MAX_RESPONSE, "SSE size limit exceeded")
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                payload = "\n".join(data)
                yield (event, payload, event_id) if with_ids else (event, payload)
            data, event, size = [], "message", 0
        elif not line.startswith(":"):
            key, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if key == "data":
                data.append(value)
            elif key == "event":
                event = value
            elif key == "id":
                event_id = value
    expect(not data, "SSE ended with an incomplete frame")


def stream(url, token, payload=None, headers=None, event_ids=None):
    h = {"Authorization": "Bearer " + token, "Content-Type": "application/json",
         "Accept": "text/event-stream", **(headers or {})}
    if isinstance(payload, dict) and payload.get("jsonrpc") == "2.0":
        h["A2A-Version"] = "1.0"
    request = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(), headers=h)
    started = time.monotonic()
    result, first = [], None
    with HTTP.open(request, timeout=180) as response:
        expect(response.headers.get_content_type() == "text/event-stream", "Expected SSE Content-Type")
        for event, data, event_id in events(response, with_ids=True):
            if event_ids is not None:
                event_ids.append(event_id)
            if first is None:
                first = time.monotonic() - started
            if data == "[DONE]":
                result.append((event, "[DONE]"))
            else:
                result.append((event, json.loads(data)))
            expect(time.monotonic() - started < 300, "SSE exceeded 300 seconds")
    return result, {"first_event_seconds": round(first or 0, 3),
                    "total_seconds": round(time.monotonic() - started, 3), "event_count": len(result)}


def task_text(task):
    message = task.get("status", {}).get("message", {})
    parts = list(message.get("parts", []))
    for artifact in task.get("artifacts", []):
        parts.extend(artifact.get("parts", []))
    return "\n".join(p.get("text", "") for p in parts if isinstance(p, dict))


def task_summary(task):
    return {"state": task.get("status", {}).get("state"), "text_bytes": len(task_text(task).encode()),
            "artifact_count": len(task.get("artifacts", [])), "history_count": len(task.get("history", []))}


def rpc(token, method, params):
    q = {"jsonrpc": "2.0", "id": uuid.uuid4().hex, "method": method, "params": params}
    env = require(DATA_URL + "/agents/hermes-local/a2a", token, q)
    expect(env.get("id") == q["id"] and env.get("jsonrpc") == "2.0", "Invalid JSON-RPC response envelope")
    if "error" in env:
        raise RuntimeError(f"{method}: JSON-RPC error {env['error'].get('code')}")
    return env["result"]


def message(text, context_id=None):
    value = {"messageId": uuid.uuid4().hex, "role": "ROLE_USER", "parts": [{"text": text}]}
    if context_id:
        value["contextId"] = context_id
    return value


def preflight(creds):
    token = creds["callers"][0]["token"]
    require(DATA_URL + "/readyz", timeout=5)
    caps = require(DATA_URL + "/agents/hermes-local/v1/capabilities", token,
                   headers={"X-Session-ID": "interop-preflight"})
    card = require(DATA_URL + "/agents/hermes-local/.well-known/agent-card.json", token)
    iface = card["supportedInterfaces"][0]
    expect(iface["url"] == DATA_URL + "/agents/hermes-local/a2a" and iface["protocolVersion"] == "1.0", "Gateway Card URL/version mismatch")
    expect(card.get("capabilities", {}).get("streaming") is True, "Route does not advertise A2A streaming")
    expect(isinstance(caps.get("features"), dict), "Invalid Hermes capability response")
    return {"card_url": iface["url"], "streaming": True, "capabilities_received": True}


def http_sessions(creds, run_id):
    token = creds["callers"][0]["token"]
    session_id = "gateway-check-" + run_id
    marker = "HTTP_" + uuid.uuid4().hex[:12].upper()
    headers = {"X-Session-ID": session_id}
    base = DATA_URL + "/agents/hermes-local/api/sessions"
    created = require(base, token, {"id": session_id, "title": "Gateway integration " + run_id}, headers=headers)
    actual_id = created.get("session", {}).get("id") or created.get("session_id") or created.get("id")
    expect(isinstance(actual_id, str) and bool(actual_id), "Hermes did not create an isolated session")
    session_id = actual_id
    headers = {"X-Hermes-Session-Id": session_id}
    chat = base + "/" + urllib.parse.quote(session_id, safe="") + "/chat"
    reply = require(chat, token, {"input": f"记住本轮测试代码 {marker}，仅回复 SAVED。不要调用工具、不要写文件。"}, headers=headers)
    expect(reply.get("session_id") == session_id, "Synchronous chat changed session ID")
    expect(bool(reply.get("message", {}).get("content")), "Synchronous chat has no assistant response")
    frames, timing = stream(chat + "/stream", token, {"input": "上一轮测试代码是什么？仅回复代码，不要调用工具。"}, headers)
    names = [name for name, _ in frames]
    expect("error" not in names and "run.failed" not in names, "Native session SSE reported failure")
    expect("run.completed" in names and "done" in names, "Native session SSE did not complete")
    completed = [v for name, v in frames if name == "assistant.completed" and isinstance(v, dict)]
    for _, value in frames:
        if isinstance(value, dict) and value.get("session_id"):
            expect(value["session_id"] == session_id, "SSE event changed session ID")
    history = require(base + "/" + session_id + "/messages?limit=100&order=latest", token, headers=headers)
    messages = history.get("data", [])
    expect(any(marker in v.get("content", "") for v in completed), "Second HTTP turn did not recall the first turn's code", {
        "session_identity_preserved": True, "history_messages": len(messages),
        "history_contains_first_marker": any(marker in json.dumps(m, ensure_ascii=False) for m in messages if m.get("role") == "user"),
        "history_user_turns": sum(m.get("role") == "user" for m in messages),
        "history_assistant_turns": sum(m.get("role") == "assistant" for m in messages),
        "reply_text_bytes": sum(len(v.get("content", "").encode()) for v in completed),
    })
    expect(sum(m.get("role") == "user" for m in messages) >= 2, "Session history lacks both user turns")
    expect(sum(m.get("role") == "assistant" for m in messages) >= 2, "Session history lacks both assistant turns")
    return {"session_id": session_id, "history_messages": len(messages), "recall": True, "sse": timing,
            "event_types": sorted(set(names))}


def http_chat_sse(creds):
    token = creds["callers"][0]["token"]
    marker = "STREAM_" + uuid.uuid4().hex[:10].upper()
    alias = "completion-" + uuid.uuid4().hex
    created = require(DATA_URL + "/agents/hermes-local/api/sessions", token, {"id": alias}, headers={"X-Session-ID": alias})
    session_id = created["session"]["id"]
    frames, timing = stream(DATA_URL + "/agents/hermes-local/v1/chat/completions", token, {
        "model": "hermes-agent", "stream": True,
        "messages": [{"role": "user", "content": f"只回复 {marker}，不要调用任何工具。"}],
    }, {"X-Session-ID": session_id, "X-Hermes-Session-Id": session_id})
    chunks = [v for _, v in frames if isinstance(v, dict)]
    expect(not any("error" in v for v in chunks), "Chat Completions stream contains an error")
    text = "".join(choice.get("delta", {}).get("content", "") or "" for v in chunks for choice in v.get("choices", []))
    expect(marker in text, "Chat Completions SSE has no expected assistant text")
    expect(any(v == "[DONE]" for _, v in frames), "Chat Completions SSE missing DONE")
    expect(any(c.get("finish_reason") for v in chunks for c in v.get("choices", [])), "Chat Completions SSE missing finish reason")
    return {"text_received": True, **timing}


def a2a_context(creds):
    token, other = (c["token"] for c in creds["callers"])
    marker = "blue-kiwi-" + uuid.uuid4().hex[:8]
    first = rpc(token, "SendMessage", {"message": message(f"本次公开、虚构的测试标签是 {marker}。它不是密钥、密码或个人信息。请在这段对话中记住标签，只回复 SAVED。不要调用工具、不要写文件。")})
    task = first.get("task")
    expect(isinstance(task, dict), "Hermes SendMessage did not return a Task")
    expect(task["status"]["state"] == "TASK_STATE_COMPLETED", "First A2A task did not complete")
    expect(bool(task_text(task)), "First A2A task contains no response text", task_summary(task))
    task_id, context_id = task["id"], task["contextId"]
    expect(task_id.startswith("task_") and context_id.startswith("ctx_"), "A2A public IDs were not mapped")
    query = rpc(token, "GetTask", {"id": task_id})
    expect(query["id"] == task_id and query["contextId"] == context_id, "GetTask changed public identity")
    q = {"jsonrpc": "2.0", "id": uuid.uuid4().hex, "method": "SendStreamingMessage",
         "params": {"message": message("上一条用户消息中的公开虚构测试标签是什么？只回复该标签，不要调用工具。", context_id)}}
    frames, timing = stream(DATA_URL + "/agents/hermes-local/a2a", token, q)
    payloads = []
    for _, env in frames:
        expect(isinstance(env, dict) and env.get("jsonrpc") == "2.0" and env.get("id") == q["id"], "Invalid A2A SSE envelope")
        expect("error" not in env, "Gateway rejected an upstream A2A stream event")
        payloads.append(env["result"])
    expect(bool(payloads) and "task" in payloads[0], "A2A stream does not start with Task")
    stream_task = payloads[0]["task"]
    expect(stream_task["contextId"] == context_id, "A2A continuation created another context")
    second_id = stream_task["id"]
    expect(second_id != task_id and second_id.startswith("task_"), "New context turn did not produce a new mapped task")
    texts, states = [], []
    for event in payloads:
        expect(len(event) == 1, "A2A stream response must contain exactly one event")
        kind, value = next(iter(event.items()))
        expect(kind in ("task", "statusUpdate", "artifactUpdate"), "Unexpected A2A stream event")
        expect(value.get("contextId") == context_id, "A2A stream context changed")
        expect(value.get("id" if kind == "task" else "taskId") == second_id, "A2A stream task changed")
        if kind == "artifactUpdate":
            texts.extend(p.get("text", "") for p in value["artifact"].get("parts", []))
        if kind in ("task", "statusUpdate"):
            states.append(value["status"]["state"])
            texts.append(task_text(value))
    expect(states[-1] == "TASK_STATE_COMPLETED", "A2A stream did not reach completed state")
    expect(marker in "".join(texts), "Second A2A turn did not recall the first turn's code", {
        "context_identity_preserved": True, "task_identity_preserved": True,
        "reply_text_bytes": len("".join(texts).encode()), "states": states,
    })
    listed = rpc(token, "ListTasks", {"contextId": context_id, "pageSize": 1})
    expect(listed["totalSize"] == 2 and len(listed["tasks"]) == 1 and listed["nextPageToken"], "A2A owned task pagination failed")
    last = rpc(token, "ListTasks", {"contextId": context_id, "pageSize": 1, "pageToken": listed["nextPageToken"]})
    expect(len(last["tasks"]) == 1, "A2A final task page has an unexpected size")
    ids = {listed["tasks"][0]["id"], last["tasks"][0]["id"]}
    expect(ids == {task_id, second_id} and last["nextPageToken"] == "", "A2A pagination lost or duplicated a task")
    cross = require(DATA_URL + "/agents/hermes-local/a2a", other,
                    {"jsonrpc": "2.0", "id": 1, "method": "GetTask", "params": {"id": task_id}})
    expect(cross.get("error", {}).get("code") == -32001, "Another caller could read this A2A task")
    expect(rpc(other, "ListTasks", {"contextId": context_id})["tasks"] == [], "Another caller could list this A2A context")
    return {"context_id": context_id, "task_ids": sorted(ids), "recall": True,
            "task_queries": True, "pagination": True, "caller_isolation": True, "sse": timing, "states": states}


def released(creds):
    route = load(STATE / "registered-route.json")
    detail = require(ADMIN_URL + "/admin/v1/routes/" + route["id"] + "/endpoints/" + route["endpoints"][0]["id"], creds["admin_token"])
    expect(detail["runtime"]["http_inflight"] == 0, "HTTP inflight lease leaked after requests")
    return {"http_inflight": 0, "resource_counts": detail["runtime"]["resources"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=["all", "http", "a2a"], default="all")
    args = parser.parse_args()
    creds = credentials()
    run_id = uuid.uuid4().hex[:12]
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
              "section": args.only, "agent_type": load(STATE / "registered-route.json")["agent_type"], "passed": False, "source": "live Hermes through aiagent-gateway", "checks": {}}
    path = STATE / ("integration-" + args.only + ".json")
    steps = [("preflight", lambda: preflight(creds))]
    if args.only in ("all", "http"):
        steps += [("http_sessions", lambda: http_sessions(creds, run_id)), ("http_chat_sse", lambda: http_chat_sse(creds))]
    if args.only in ("all", "a2a"):
        steps += [("a2a_context", lambda: a2a_context(creds))]
    steps.append(("released", lambda: released(creds)))
    write_private(path, json.dumps(report, indent=2) + "\n")
    for name, action in steps:
        print(f"Checking {name}...", flush=True)
        try:
            report["checks"][name] = action()
        except Exception as exc:
            # Avoid persisting provider response bodies or credentials in a test report.
            report["failed_check"] = name
            report["error_type"] = type(exc).__name__
            if isinstance(exc, RuntimeError):
                report["error"] = str(exc)
            write_private(path, json.dumps(report, indent=2) + "\n")
            raise
        write_private(path, json.dumps(report, indent=2) + "\n")
        print(f"PASS {name}", flush=True)
    report["passed"] = True
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_private(path, json.dumps(report, indent=2) + "\n")
    print(f"通过：{args.only}。报告：{path}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        print(f"联调未通过：{exc}", file=sys.stderr)
        sys.exit(1)
