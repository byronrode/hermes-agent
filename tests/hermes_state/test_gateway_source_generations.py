"""Accepted source custody follows one logical message across compaction copies."""
import json

import pytest

from hermes_state import SessionDB


def source_copies(db):
    db.create_session("source", "discord")
    candidate = {"message_id": "native-message", "run_id": "native-run"}
    source = {**candidate, "session_id": "source", "platform": "discord", "turn_id": "turn",
              "provider": "existing-owner", "profile": "default"}
    db.append_message("source", "user", "original request", timestamp=100,
                      platform_message_id=candidate["message_id"],
                      display_metadata={"gateway_source_candidate": candidate,
                                        "gateway_accepted_source": source})
    carried = db.get_messages("source")[0]
    db.archive_and_compact("source", [dict(carried)], tail_count=1)
    return source, candidate, carried


@pytest.mark.parametrize("all_archived", [False, True])
def test_compaction_copies_share_poll_and_consumption_across_restart(tmp_path, all_archived):
    path = tmp_path / "state.db"
    db = SessionDB(path)
    source, candidate, carried = source_copies(db)
    rows = db._read_all("SELECT * FROM messages ORDER BY id")
    assert len(rows) == 2 and rows[0]["active"] == 0 and rows[1]["active"] == 1
    if all_archived:
        db._execute_write(lambda conn: conn.execute("UPDATE messages SET active=0,compacted=1"))
    metadata = json.loads(rows[1]["display_metadata"])
    metadata["gateway_accepted_source"] = dict(reversed(list(source.items())))
    db._execute_write(lambda conn: conn.execute("UPDATE messages SET display_metadata=? WHERE id=?",
                                                (json.dumps(metadata), rows[1]["id"])))
    assert db.gateway_source_row(source) == rows[1]["id"]
    # Key order is serialization, not custody. Rebinding never admits a new turn.
    assert db.bind_gateway_source_turn("source", candidate, "turn",
                                     {"session_id": "source", "platform": "discord",
                                      "provider": "existing-owner", "profile": "default"}) == source
    assert db.pending_gateway_sources() == [source]
    assert db.gateway_source_row(source) == rows[1]["id"]
    db.mark_gateway_source_polled(source)
    sequences = [json.loads(r[0])["gateway_source_poll_sequence"] for r in
                 db._read_all("SELECT display_metadata FROM messages")]
    assert len(set(sequences)) == 1 and sequences[0] > 0
    db.close()
    db = SessionDB(path)
    db.consume_gateway_source(source)
    assert db.pending_gateway_sources() == []
    # A compactor holding a pre-consumption snapshot cannot resurrect delivery.
    db.archive_and_compact("source", [dict(carried)], tail_count=1)
    assert db.pending_gateway_sources() == []
    assert len(db._read_all("SELECT id FROM messages")) == 3
    db.close()


@pytest.mark.parametrize("divergence", ["provider", "turn_id", "content", "session"])
def test_divergent_custody_or_logical_message_still_fails_closed(tmp_path, divergence):
    db = SessionDB(tmp_path / "state.db")
    source, candidate, _ = source_copies(db)
    last = db._read_one("SELECT id,display_metadata FROM messages ORDER BY id DESC LIMIT 1")
    meta = json.loads(last["display_metadata"])
    if divergence in {"provider", "turn_id"}:
        meta["gateway_accepted_source"][divergence] = "different-owner"
        db._execute_write(lambda conn: conn.execute("UPDATE messages SET display_metadata=? WHERE id=?",
                                                    (json.dumps(meta), last["id"])))
    elif divergence == "content":
        db._execute_write(lambda conn: conn.execute("UPDATE messages SET content=? WHERE id=?",
                                                    ("different request", last["id"])))
    else:
        source = {**source, "session_id": "missing-session"}
    for operation in (db.gateway_source_row, db.mark_gateway_source_polled, db.consume_gateway_source):
        with pytest.raises(ValueError, match="gateway_source_(custody|session)_changed"):
            operation(source)
    assert db.bind_gateway_source_turn("source", candidate, "turn",
                                     {"session_id": "source", "platform": "discord",
                                      "provider": "existing-owner", "profile": "default"}) is None or divergence == "session"
    db.close()
