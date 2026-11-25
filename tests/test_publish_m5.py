"""M5's published logs: what goes in, named so they unpack where the scores read them."""

import tarfile

import publish_m5
import publish_sessions


def test_the_archive_holds_the_briefs_and_sessions_scrubbed(tmp_path):
    out = tmp_path / "out-m5"
    logs = out / "backtest" / "briefs" / "sonnet-v2" / "logs"
    logs.mkdir(parents=True)
    (logs / "b.jsonl").write_text('{"arn": "arn:aws:sts::444455556666:x"}\n')
    (logs.parent / "2026-08-20.md").write_text("A brief.\n")
    (logs.parent / "notes.txt").write_text("not published\n")
    sessions = out / "agent" / "sessions" / "plant" / "step7"
    (sessions / "excluded-auth").mkdir(parents=True)
    (sessions / "s.jsonl").write_text("{}\n")
    (sessions / "excluded-auth" / "e.jsonl").write_text("{}\n")

    found = publish_m5.files(out)
    target = tmp_path / "release.tar.gz"
    publish_sessions.pack("444455556666", found, root=out, target=target)

    with tarfile.open(target) as tar:
        names = sorted(tar.getnames())
        log = tar.extractfile("backtest/briefs/sonnet-v2/logs/b.jsonl").read()
    assert names == [
        "agent/sessions/plant/step7/excluded-auth/e.jsonl",
        "agent/sessions/plant/step7/s.jsonl",
        "backtest/briefs/sonnet-v2/2026-08-20.md",
        "backtest/briefs/sonnet-v2/logs/b.jsonl",
    ]
    assert b"444455556666" not in log and b"123456789012" in log
