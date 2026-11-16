"""The agent's MCP client, offline (M4 step 5).

MCP's streamable HTTP may answer with an event stream that stays open after
the reply, so the client stops at the reply. A call with no reply at all (a
laptop that slept mid-call, as in step 5's first run) comes back as an error
the model sees, not an exception that ends the session.
"""

import json
import threading
from pathlib import Path

from crestline_agent import loop
from crestline_agent.gateway import first_reply


class OpenStream:
    """Event lines, then a stream that never ends: reading past them fails."""

    def __init__(self, *lines: str):
        self.lines = [f"{line}\n".encode() for line in lines]

    def __iter__(self):
        yield from self.lines
        raise AssertionError("read past the reply into a stream that stays open")


def event(message: dict) -> list[str]:
    return [f"data: {json.dumps(message)}", ""]


def test_the_reply_is_taken_as_soon_as_it_arrives():
    reply = {"jsonrpc": "2.0", "id": 7, "result": {"content": []}}
    stream = OpenStream(
        ": keep-alive",
        *event({"jsonrpc": "2.0", "method": "notifications/progress", "params": {}}),
        *event({"jsonrpc": "2.0", "id": 6, "result": {}}),  # not ours
        *event(reply),
    )
    assert first_reply(stream, 7) == reply


def test_an_event_may_span_several_data_lines():
    stream = OpenStream('data: {"jsonrpc": "2.0", "id": 3,', 'data: "error": {}}', "")
    assert first_reply(stream, 3) == {"jsonrpc": "2.0", "id": 3, "error": {}}


def test_a_stream_that_closes_without_a_blank_line_or_a_reply():
    last = [f"data: {json.dumps({'jsonrpc': '2.0', 'id': 1, 'result': {}})}".encode()]
    assert first_reply(iter(last), 1) == {"jsonrpc": "2.0", "id": 1, "result": {}}
    assert "transport_error" in first_reply(iter([b": ping\n"]), 1)


def test_a_failed_call_reaches_the_model_as_an_error():
    block = loop.tool_result("t1", {"transport_error": "timed out", "_ms": 300000})
    assert block["toolResult"]["status"] == "error"
    assert "timed out" in block["toolResult"]["content"][0]["text"]


def test_sessions_started_together_never_share_a_log(tmp_path: Path):
    names: list[Path] = []
    lock = threading.Lock()

    def start():
        _, path = loop._new_log(tmp_path, loop.MODELS["sonnet"], "ops-analyst")
        with lock:
            names.append(path)

    threads = [threading.Thread(target=start) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(names)) == 8
