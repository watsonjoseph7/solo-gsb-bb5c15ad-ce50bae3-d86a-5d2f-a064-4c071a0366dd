"""Behaviour tests for cross-source composite (correlated) alert rules."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SENSOR_DB_PATH", db_path)
    from app import database, main

    main.app.state.db_path = db_path
    database.init_db(db_path)
    with TestClient(main.app) as c:
        yield c


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)

# Benign values that never breach the default conditions
# (a: above 30, b: below 5, c: above 100).
BENIGN = {"a": 0, "b": 100}
BENIGN_C = {**BENIGN, "c": 0}


def at(second: int) -> str:
    return (BASE + timedelta(seconds=second)).strftime("%Y-%m-%dT%H:%M:%SZ")


def report(client, sid, readings):
    return client.post(f"/sessions/{sid}/readings", json={"readings": readings})


def step(client, sid, second, values, seq):
    """Report one reading per source at ``second`` (zero allowed lateness).

    The new watermark equals ``second`` once every source has reported, so
    readings reported here sit on the boundary and stay buffered until a
    later step pushes the watermark past them.
    """
    rows = []
    for s, v in values.items():
        n = seq.get(s, 0) + 1
        seq[s] = n
        rows.append({"source_id": s, "seq": n, "observed_at": at(second), "value": v})
    return report(client, sid, rows)


def create_session(client, sources=("a", "b"), lateness=0.0):
    r = client.post("/sessions", json={
        "source_ids": list(sources),
        "allowed_lateness_seconds": lateness,
    })
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def make_rule(client, sid, conditions=None, window=30.0, rule_id=None):
    body = {
        "conditions": conditions or [
            {"source_id": "a", "direction": "above", "threshold": 30.0},
            {"source_id": "b", "direction": "below", "threshold": 5.0},
        ],
        "window_seconds": window,
    }
    if rule_id is not None:
        body["rule_id"] = rule_id
    r = client.post(f"/sessions/{sid}/correlation-rules", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def hits(client, sid, rid):
    r = client.get(f"/sessions/{sid}/correlation-rules/{rid}/hits")
    assert r.status_code == 200, r.text
    return r.json()


def corr_history(client, sid, **params):
    """Page through the whole correlation event history."""
    out = []
    cursor = None
    for _ in range(100):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/correlation-alerts/history", params=q).json()
        out.extend(page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            return out
    raise AssertionError("pagination did not terminate")


class TestRuleValidation:
    def test_condition_count_bounds_422(self, client):
        sid = create_session(client)
        one = [{"source_id": "a", "direction": "above", "threshold": 30}]
        r = client.post(f"/sessions/{sid}/correlation-rules",
                        json={"conditions": one, "window_seconds": 30})
        assert r.status_code == 422
        five = [
            {"source_id": f"s{i}", "direction": "above", "threshold": 1}
            for i in range(5)
        ]
        r = client.post(f"/sessions/{sid}/correlation-rules",
                        json={"conditions": five, "window_seconds": 30})
        assert r.status_code == 422

    def test_duplicate_sources_in_conditions_422(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [
                {"source_id": "a", "direction": "above", "threshold": 30},
                {"source_id": "a", "direction": "below", "threshold": 0},
            ],
            "window_seconds": 30,
        })
        assert r.status_code == 422

    def test_unknown_source_400(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [
                {"source_id": "a", "direction": "above", "threshold": 30},
                {"source_id": "zzz", "direction": "below", "threshold": 5},
            ],
            "window_seconds": 30,
        })
        assert r.status_code == 400
        assert "unknown source" in r.json()["detail"]

    def test_bad_window_422(self, client):
        sid = create_session(client)
        conditions = [
            {"source_id": "a", "direction": "above", "threshold": 30},
            {"source_id": "b", "direction": "below", "threshold": 5},
        ]
        for bad in (0, -5, "NaN", "Infinity", "-Infinity"):
            r = client.post(f"/sessions/{sid}/correlation-rules",
                            json={"conditions": conditions, "window_seconds": bad})
            assert r.status_code == 422, bad

    def test_bad_condition_fields_422(self, client):
        sid = create_session(client)
        good = {"source_id": "a", "direction": "above", "threshold": 30}
        other = {"source_id": "b", "direction": "below", "threshold": 5}
        # bad direction
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [{**good, "direction": "sideways"}, other],
            "window_seconds": 30,
        })
        assert r.status_code == 422
        # missing threshold
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [{"source_id": "a", "direction": "above"}, other],
            "window_seconds": 30,
        })
        assert r.status_code == 422
        # non-finite threshold
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [{**good, "threshold": "NaN"}, other],
            "window_seconds": 30,
        })
        assert r.status_code == 422
        # blank source / value_key
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [{**good, "source_id": "  "}, other],
            "window_seconds": 30,
        })
        assert r.status_code == 422
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "conditions": [{**good, "value_key": "  "}, other],
            "window_seconds": 30,
        })
        assert r.status_code == 422

    def test_rule_on_unknown_session_404(self, client):
        r = client.post("/sessions/nope/correlation-rules", json={
            "conditions": [
                {"source_id": "a", "direction": "above", "threshold": 30},
                {"source_id": "b", "direction": "below", "threshold": 5},
            ],
            "window_seconds": 30,
        })
        assert r.status_code == 404

    def test_duplicate_rule_id_409(self, client):
        sid = create_session(client)
        make_rule(client, sid, rule_id="r1")
        r = client.post(f"/sessions/{sid}/correlation-rules", json={
            "rule_id": "r1",
            "conditions": [
                {"source_id": "a", "direction": "above", "threshold": 30},
                {"source_id": "b", "direction": "below", "threshold": 5},
            ],
            "window_seconds": 10,
        })
        assert r.status_code == 409

    def test_list_get_and_hits_of_rules(self, client):
        sid = create_session(client)
        make_rule(client, sid, rule_id="r1", window=30)
        make_rule(client, sid, rule_id="r2", window=60)
        rules = client.get(f"/sessions/{sid}/correlation-rules").json()["rules"]
        assert {r["rule_id"] for r in rules} == {"r1", "r2"}
        one = client.get(f"/sessions/{sid}/correlation-rules/r1")
        assert one.status_code == 200
        body = one.json()
        assert body["window_seconds"] == 30.0
        assert body["state"] == "waiting"
        assert [c["source_id"] for c in body["conditions"]] == ["a", "b"]
        # fresh rule: no hits yet
        h = hits(client, sid, "r1")
        assert h["state"] == "waiting"
        assert h["satisfied"] is False
        assert h["window_start"] is None and h["window_end"] is None
        assert [c["last_hit"] for c in h["conditions"]] == [None, None]
        # unknown rules / sessions -> 404
        assert client.get(f"/sessions/{sid}/correlation-rules/ghost").status_code == 404
        assert client.get(f"/sessions/{sid}/correlation-rules/ghost/hits").status_code == 404
        assert client.get("/sessions/nope/correlation-rules").status_code == 404
        assert client.get("/sessions/nope/correlation-alerts/history").status_code == 404


class TestCorrelation:
    def test_enter_when_all_conditions_hit_within_window(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=30)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)   # a breaches @1
        step(client, sid, 2, {"a": 0, "b": 100}, seq)    # publishes a@1
        h = hits(client, sid, rid)
        assert h["state"] == "waiting"                   # b has not hit yet
        assert h["conditions"][0]["last_hit"]["observed_at"] == at(1)
        assert h["conditions"][1]["last_hit"] is None
        step(client, sid, 3, {"a": 0, "b": 2}, seq)      # b@3 breaches, still buffered
        h = hits(client, sid, rid)
        assert h["state"] == "waiting"
        step(client, sid, 4, BENIGN, seq)                # publishes b@3 -> aligned
        h = hits(client, sid, rid)
        assert h["state"] == "correlated"
        assert h["satisfied"] is True
        assert h["window_start"] == at(1)
        assert h["window_end"] == at(3)
        hit_a, hit_b = h["conditions"]
        assert hit_a["last_hit"] == {
            "source_id": "a", "seq": 1, "observed_at": at(1), "value": 35.0,
        }
        assert hit_b["last_hit"] == {
            "source_id": "b", "seq": 3, "observed_at": at(3), "value": 2.0,
        }
        events = corr_history(client, sid)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_type"] == "correlated_entered"
        assert ev["rule_id"] == rid
        assert ev["window_seconds"] == 30.0
        assert ev["window_start"] == at(1)
        assert ev["window_end"] == at(3)
        assert [(x["source_id"], x["seq"], x["value"]) for x in ev["hits"]] == [
            ("a", 1, 35.0), ("b", 3, 2.0),
        ]

    def test_no_event_when_hits_outside_window(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=5)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 100}, seq)    # hit a@1
        step(client, sid, 10, {"a": 0, "b": 2}, seq)
        step(client, sid, 11, {"a": 0, "b": 100}, seq)   # hit b@10, spread 9 > 5
        h = hits(client, sid, rid)
        assert h["state"] == "waiting"
        assert h["satisfied"] is False
        assert h["conditions"][0]["last_hit"]["observed_at"] == at(1)
        assert h["conditions"][1]["last_hit"]["observed_at"] == at(10)
        assert corr_history(client, sid) == []

    def test_window_boundary_is_inclusive(self, client):
        sid = create_session(client)
        make_rule(client, sid, window=9)
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 100}, seq)    # hit a@1
        step(client, sid, 10, {"a": 0, "b": 2}, seq)
        step(client, sid, 11, {"a": 0, "b": 100}, seq)   # hit b@10, spread == 9
        events = corr_history(client, sid)
        assert len(events) == 1
        assert events[0]["window_start"] == at(1)
        assert events[0]["window_end"] == at(10)

    def test_exit_and_reenter_produces_second_event(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=10)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 2}, seq)
        step(client, sid, 3, {"a": 0, "b": 100}, seq)    # enter (hits a@1, b@2)
        assert hits(client, sid, rid)["state"] == "correlated"
        step(client, sid, 20, {"a": 35, "b": 100}, seq)
        step(client, sid, 21, {"a": 0, "b": 100}, seq)   # hit a@20, spread 18 -> exit
        h = hits(client, sid, rid)
        assert h["state"] == "waiting"
        assert len(corr_history(client, sid)) == 1       # exit emits nothing
        step(client, sid, 22, {"a": 0, "b": 2}, seq)
        step(client, sid, 23, {"a": 0, "b": 100}, seq)   # hit b@22, spread 2 -> re-enter
        events = corr_history(client, sid)
        assert [e["window_end"] for e in events] == [at(22), at(2)]
        assert hits(client, sid, rid)["state"] == "correlated"

    def test_no_duplicate_events_while_aligned(self, client):
        sid = create_session(client)
        make_rule(client, sid, window=30)
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 2}, seq)
        step(client, sid, 3, {"a": 0, "b": 100}, seq)    # enter
        # further breaches that keep the spread inside the window must not
        # produce additional events
        step(client, sid, 4, {"a": 35, "b": 100}, seq)
        step(client, sid, 5, {"a": 0, "b": 2}, seq)      # hit a@4
        step(client, sid, 6, {"a": 0, "b": 100}, seq)    # hit b@5
        assert len(corr_history(client, sid)) == 1

    def test_merged_order_exit_and_reentry_within_one_publish(self, client):
        sid = create_session(client)
        make_rule(client, sid, window=2)
        # everything buffers while b stays silent, then one batch publishes
        # a@1, b@1, a@5, b@6 in merged order
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 35},
            {"source_id": "a", "seq": 2, "observed_at": at(5), "value": 35},
            {"source_id": "a", "seq": 3, "observed_at": at(7), "value": 0},
        ])
        assert r.json()["published_now"] == 0
        r = report(client, sid, [
            {"source_id": "b", "seq": 1, "observed_at": at(1), "value": 2},
            {"source_id": "b", "seq": 2, "observed_at": at(6), "value": 2},
            {"source_id": "b", "seq": 3, "observed_at": at(8), "value": 100},
        ]).json()
        assert r["published_now"] == 4
        # a@1+b@1 align (spread 0) -> enter; a@5 pushes spread to 4 -> exit;
        # b@6 re-aligns with a@5 (spread 1) -> second enter
        events = corr_history(client, sid)
        assert [e["window_end"] for e in events] == [at(6), at(1)]
        assert events[0]["window_start"] == at(5)

    def test_three_conditions_all_required(self, client):
        sid = create_session(client, sources=("a", "b", "c"))
        rid = make_rule(client, sid, conditions=[
            {"source_id": "a", "direction": "above", "threshold": 30.0},
            {"source_id": "b", "direction": "below", "threshold": 5.0},
            {"source_id": "c", "direction": "above", "threshold": 100.0},
        ], window=30)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100, "c": 0}, seq)
        step(client, sid, 2, {"a": 0, "b": 2, "c": 0}, seq)
        step(client, sid, 3, BENIGN_C, seq)              # hits a@1, b@2; c missing
        h = hits(client, sid, rid)
        assert h["state"] == "waiting"
        assert h["conditions"][2]["last_hit"] is None
        step(client, sid, 4, {"a": 0, "b": 100, "c": 150}, seq)
        step(client, sid, 5, BENIGN_C, seq)              # hit c@4 -> aligned
        events = corr_history(client, sid)
        assert len(events) == 1
        assert [x["source_id"] for x in events[0]["hits"]] == ["a", "b", "c"]
        assert events[0]["window_start"] == at(1)
        assert events[0]["window_end"] == at(4)


class TestHitInputs:
    def test_value_key_extracts_from_object_payload(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, conditions=[
            {"source_id": "a", "direction": "above", "threshold": 30.0,
             "value_key": "temp"},
            {"source_id": "b", "direction": "below", "threshold": 5.0,
             "value_key": "temp"},
        ], window=30)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": {"temp": 35, "unit": "C"}, "b": {"temp": 100}}, seq)
        step(client, sid, 2, {"a": {"temp": 0}, "b": {"other": 1}}, seq)  # hit a@1
        step(client, sid, 3, {"a": {"temp": 0}, "b": {"temp": 2}}, seq)   # b@2: key missing
        h = hits(client, sid, rid)
        assert h["state"] == "waiting"
        assert h["conditions"][1]["last_hit"] is None
        step(client, sid, 4, {"a": {"temp": 0}, "b": {"temp": 2}}, seq)   # hit b@3
        events = corr_history(client, sid)
        assert len(events) == 1
        assert [(x["source_id"], x["value"]) for x in events[0]["hits"]] == [
            ("a", 35.0), ("b", 2.0),
        ]

    def test_non_numeric_values_never_hit(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=30)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": "hot", "b": 100}, seq)
        step(client, sid, 2, {"a": True, "b": 100}, seq)
        step(client, sid, 3, {"a": None, "b": 100}, seq)
        step(client, sid, 4, {"a": 35, "b": 2}, seq)
        step(client, sid, 5, BENIGN, seq)                # publishes through @4
        h = hits(client, sid, rid)
        # only the numeric breach a@4 counts as a hit
        assert h["conditions"][0]["last_hit"]["observed_at"] == at(4)
        assert h["state"] == "correlated"
        assert len(corr_history(client, sid)) == 1

    def test_strict_threshold_equality_does_not_hit(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=30)["rule_id"]
        seq: dict = {}
        step(client, sid, 1, {"a": 30, "b": 5}, seq)     # exactly on the thresholds
        step(client, sid, 2, BENIGN, seq)
        h = hits(client, sid, rid)
        assert [c["last_hit"] for c in h["conditions"]] == [None, None]
        assert corr_history(client, sid) == []


class TestPublishedOnly:
    def test_quarantined_late_reading_never_hits(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=30)["rule_id"]
        seq: dict = {}
        step(client, sid, 10, {"a": 0, "b": 100}, seq)   # published watermark :10
        # a breaching reading arriving behind the watermark is quarantined
        r = report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": at(5), "value": 35},
        ]).json()
        assert r["quarantined"] == 1
        seq["a"] = 2  # the quarantined reading still consumed a's seq 2
        step(client, sid, 11, {"a": 0, "b": 2}, seq)
        step(client, sid, 12, BENIGN, seq)               # hit b@11 only
        h = hits(client, sid, rid)
        assert h["conditions"][0]["last_hit"] is None
        assert h["conditions"][1]["last_hit"]["observed_at"] == at(11)
        assert h["state"] == "waiting"
        assert corr_history(client, sid) == []
        q = client.get(f"/sessions/{sid}/quarantine").json()["readings"]
        assert len(q) == 1 and q[0]["value"] == 35

    def test_buffered_readings_do_not_hit_until_published(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, window=30)["rule_id"]
        seq: dict = {}
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 35},
        ])
        assert hits(client, sid, rid)["conditions"][0]["last_hit"] is None
        report(client, sid, [
            {"source_id": "b", "seq": 1, "observed_at": at(1), "value": 100},
        ])
        # a@1 sits exactly on the watermark: still buffered, still no hit
        assert hits(client, sid, rid)["conditions"][0]["last_hit"] is None
        seq = {"a": 1, "b": 1}  # both direct reports above consumed seq 1
        step(client, sid, 2, BENIGN, seq)                # publishes a@1
        h = hits(client, sid, rid)
        assert h["conditions"][0]["last_hit"]["observed_at"] == at(1)
        assert h["state"] == "waiting"                   # b has not hit

    def test_duplicate_reports_do_not_retrigger(self, client):
        sid = create_session(client)
        make_rule(client, sid, window=30)
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 2}, seq)
        step(client, sid, 3, BENIGN, seq)                # enter
        assert len(corr_history(client, sid)) == 1
        # re-post the identical breaching readings: duplicates, no new events
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 35},
            {"source_id": "b", "seq": 2, "observed_at": at(2), "value": 2},
        ]).json()
        assert r["duplicates"] == 2
        assert len(corr_history(client, sid)) == 1


class TestBackfill:
    def test_rule_created_after_publication_replays(self, client):
        sid = create_session(client)
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 2}, seq)
        step(client, sid, 3, BENIGN, seq)                # a@1, b@2 published
        created = make_rule(client, sid, window=30)
        assert created["state"] == "correlated"
        events = corr_history(client, sid)
        assert len(events) == 1
        assert events[0]["window_start"] == at(1)
        assert events[0]["window_end"] == at(2)

    def test_backfill_respects_window(self, client):
        sid = create_session(client)
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, BENIGN, seq)                # publishes a@1
        step(client, sid, 10, {"a": 0, "b": 2}, seq)
        step(client, sid, 11, BENIGN, seq)               # publishes b@10
        created = make_rule(client, sid, window=5)       # spread 9 > 5
        assert created["state"] == "waiting"
        assert corr_history(client, sid) == []


class TestHistoryPagination:
    def _three_events(self, client, sid):
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 2}, seq)
        step(client, sid, 3, BENIGN, seq)                # enter #1 (end :02)
        step(client, sid, 20, {"a": 35, "b": 100}, seq)
        step(client, sid, 21, BENIGN, seq)               # hit a@20 -> exit
        step(client, sid, 22, {"a": 0, "b": 2}, seq)
        step(client, sid, 23, BENIGN, seq)               # enter #2 (end :22)
        step(client, sid, 40, {"a": 35, "b": 100}, seq)
        step(client, sid, 41, BENIGN, seq)               # hit a@40 -> exit
        step(client, sid, 42, {"a": 0, "b": 2}, seq)
        step(client, sid, 43, BENIGN, seq)               # enter #3 (end :42)

    def test_keyset_pagination_newest_first(self, client):
        sid = create_session(client)
        make_rule(client, sid, window=10)
        self._three_events(client, sid)
        events = corr_history(client, sid, limit=2)
        assert len(events) == 3
        ids = [e["id"] for e in events]
        assert ids == sorted(ids, reverse=True)
        assert len(set(ids)) == 3
        assert [e["window_end"] for e in events] == [at(42), at(22), at(2)]

    def test_bad_cursor_400(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/correlation-alerts/history",
                       params={"cursor": "%%%garbage"})
        assert r.status_code == 400

    def test_rule_id_filter_and_unknown_rule_404(self, client):
        sid = create_session(client)
        make_rule(client, sid, rule_id="r1", window=30)
        # r2 can never enter: b=2 is not below 0
        make_rule(client, sid, conditions=[
            {"source_id": "a", "direction": "above", "threshold": 30.0},
            {"source_id": "b", "direction": "below", "threshold": 0.0},
        ], rule_id="r2", window=30)
        seq: dict = {}
        step(client, sid, 1, {"a": 35, "b": 100}, seq)
        step(client, sid, 2, {"a": 0, "b": 2}, seq)
        step(client, sid, 3, BENIGN, seq)
        assert len(corr_history(client, sid, rule_id="r1")) == 1
        assert corr_history(client, sid, rule_id="r2") == []
        r = client.get(f"/sessions/{sid}/correlation-alerts/history",
                       params={"rule_id": "ghost"})
        assert r.status_code == 404
