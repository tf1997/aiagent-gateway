#!/usr/bin/env python3
"""Official a2a-sdk 1.2.0 against the gateway and both real Hermes instances."""
import asyncio
from datetime import datetime, timezone
from importlib.metadata import version
import json
import uuid
import httpx
from a2a.client import A2ACardResolver, ClientFactory, ClientConfig
from a2a import types
from gateway import ADMIN_URL, DATA_URL, STATE, credentials, require
from manage import write_private
from check_gateway import expect


def request(text, context="", immediate=False):
    return types.SendMessageRequest(
        message=types.Message(message_id=uuid.uuid4().hex, context_id=context,
                              role=types.Role.ROLE_USER, parts=[types.Part(text=text)]),
        configuration=types.SendMessageConfiguration(return_immediately=immediate))


async def main():
    expect(version("a2a-sdk") == "1.2.0", "Expected pinned official SDK 1.2.0")
    creds = credentials()
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "passed": False,
              "sdk_version": version("a2a-sdk"), "checks": {}}
    path = STATE / "integration-sdk.json"
    def record(name, value):
        report["checks"][name] = value
        write_private(path, json.dumps(report, indent=2) + "\n")
        print("PASS " + name, flush=True)
    write_private(path, json.dumps(report, indent=2) + "\n")
    try:
        async with httpx.AsyncClient(timeout=180, trust_env=False, headers={
            "Authorization": "Bearer " + creds["callers"][0]["token"], "A2A-Version": "1.0"
        }) as http:
            card = await A2ACardResolver(http, DATA_URL + "/agents/hermes-local").get_agent_card()
            expect(card.supported_interfaces[0].url == DATA_URL + "/agents/hermes-local/a2a", "Card has wrong public URL")
            sync = ClientFactory(ClientConfig(httpx_client=http, streaming=False, supported_protocol_bindings=["JSONRPC"])).create(card)
            streaming = ClientFactory(ClientConfig(httpx_client=http, streaming=True, supported_protocol_bindings=["JSONRPC"])).create(card)
            record("Card", {"version": card.supported_interfaces[0].protocol_version})
            print("Checking SendMessage...", flush=True)
            events = [event async for event in sync.send_message(request("请只回复 SDK_READY。这是公开的接口测试，不要调用工具。"))]
            expect(len(events) == 1 and events[0].HasField("task"), "SDK sync send lacks Task")
            first = events[0].task
            expect(first.status.state == types.TaskState.TASK_STATE_COMPLETED, "SDK task did not complete")
            record("SendMessage", {"task_id": first.id, "context_id": first.context_id})
            fetched = await sync.get_task(types.GetTaskRequest(id=first.id))
            expect(fetched.id == first.id and fetched.context_id == first.context_id, "SDK GetTask changed identity")
            record("GetTask", True)
            print("Checking SendStreamingMessage...", flush=True)
            events = [event async for event in streaming.send_message(request("请只回复 SDK_STREAM_READY，不要调用工具。", first.context_id))]
            expect(events and events[0].HasField("task"), "SDK stream lacks initial Task")
            second = events[0].task
            expect(second.context_id == first.context_id and second.id != first.id, "SDK context did not continue")
            expect(events[-1].HasField("status_update") and events[-1].status_update.status.state == types.TaskState.TASK_STATE_COMPLETED, "SDK stream lacks completion")
            record("SendStreamingMessage", {"task_id": second.id, "event_count": len(events)})
            listed = await sync.list_tasks(types.ListTasksRequest(context_id=first.context_id, page_size=1))
            last = await sync.list_tasks(types.ListTasksRequest(context_id=first.context_id, page_size=1, page_token=listed.next_page_token))
            expect(listed.total_size == 2 and {listed.tasks[0].id, last.tasks[0].id} == {first.id, second.id}, "SDK pagination mismatch")
            record("ListTasks", True)
            print("Checking SubscribeToTask and CancelTask...", flush=True)
            submitted = [event async for event in sync.send_message(request("请用中文写一小段关于春天的描述，不要调用工具。", immediate=True))]
            pending = submitted[0].task
            subscription = streaming.subscribe(types.SubscribeToTaskRequest(id=pending.id))
            try:
                initial = await asyncio.wait_for(anext(subscription), timeout=30)
                expect(initial.HasField("task") and initial.task.id == pending.id, "SubscribeToTask lacks mapped initial Task")
                canceled = await sync.cancel_task(types.CancelTaskRequest(id=pending.id))
                expect(canceled.status.state == types.TaskState.TASK_STATE_CANCELED, "CancelTask failed")
                rest = [event async for event in subscription]
                expect(rest and rest[-1].HasField("status_update") and rest[-1].status_update.status.state == types.TaskState.TASK_STATE_CANCELED, "Subscription did not observe cancellation")
            finally:
                await subscription.aclose()
            record("SubscribeToTask", {"initial_task": True, "canceled_event": True})
            record("CancelTask", {"task_id": pending.id})
            task_ids = {first.id, second.id, pending.id}
            route = require(ADMIN_URL + "/admin/v1/routes", creds["admin_token"])["items"][0]
            resources = require(ADMIN_URL + "/admin/v1/routes/" + route["id"] + "/resources?resource_kind=a2a_task", creds["admin_token"])["items"]
            endpoints = {item["endpoint_id"] for item in resources if item["public_id"] in task_ids}
            expect(len(endpoints) == 2, "SDK calls did not exercise both Hermes instances")
            record("two_instances", {"endpoint_ids": sorted(endpoints)})
        report["passed"] = True
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_private(path, json.dumps(report, indent=2) + "\n")
        print("通过：官方 SDK 的 Card 和全部六个 RPC，覆盖两个真实 Hermes 实例。", flush=True)
    except Exception as exc:
        report["error_type"] = type(exc).__name__
        if isinstance(exc, RuntimeError):
            report["error"] = str(exc)
        write_private(path, json.dumps(report, indent=2) + "\n")
        raise


if __name__ == "__main__":
    asyncio.run(main())
