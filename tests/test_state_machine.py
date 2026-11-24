"""Tests for the Step Functions definition and the local interpreter (no Spark needed)."""

import json
from pathlib import Path

import pytest

import run_local
from run_local import LocalStateMachine, StateMachineFailed, evaluate

ASL = Path(__file__).resolve().parent.parent / "orchestration" / "claims_pipeline.asl.json"


def load_definition():
    return json.loads(ASL.read_text().replace("${job_prefix}", "claims").replace("${sns_topic_arn}", "arn:test"))


class FakeJob:
    DEFAULTS = {}

    def __init__(self, calls, fail_with=None):
        self.calls, self.fail_with = calls, fail_with

    def run(self, spark, args):
        self.calls.append(args)
        if self.fail_with:
            raise self.fail_with
        return {"ok": True}


def machine_with(monkeypatch, fail_job=None, error=None):
    calls = {}

    def fake_import(name):
        calls.setdefault(name, [])
        return FakeJob(calls[name], error if name == fail_job else None)

    monkeypatch.setattr(run_local.importlib, "import_module", fake_import)
    logs = []
    return LocalStateMachine(load_definition(), None, "lake", "landing", logs.append), calls, logs


def test_every_glue_task_has_a_local_job():
    text = ASL.read_text()
    for job in run_local.JOB_MODULES:
        assert job.replace("claims", "${job_prefix}") in text


def test_happy_path_runs_all_jobs_in_order_and_notifies(monkeypatch):
    machine, calls, _ = machine_with(monkeypatch)
    machine.run({"run_date": "2026-10-21"})
    assert list(calls) == ["job_bronze_batch", "job_silver", "job_gold_dim_member", "job_gold_dimensions",
                           "job_gold_facts", "job_data_quality"]
    assert all(c[0]["run_date"] == "2026-10-21" for c in calls.values())
    assert machine.notifications[-1]["Subject"] == "Claims pipeline succeeded"


def test_scheduled_run_without_date_uses_execution_start_date(monkeypatch):
    machine, calls, _ = machine_with(monkeypatch)
    machine.context["Execution"]["StartTime"] = "2026-10-22T10:00:00.000Z"
    machine.run({})
    assert calls["job_silver"][0]["run_date"] == "2026-10-22"


def test_failure_is_caught_alerted_and_stops_later_steps(monkeypatch):
    class DataQualityError(Exception):
        pass

    machine, calls, _ = machine_with(monkeypatch, "job_silver", DataQualityError("bad data"))
    with pytest.raises(StateMachineFailed):
        machine.run({"run_date": "2026-10-21"})
    assert "job_gold_facts" not in calls
    alert = machine.notifications[-1]
    assert alert["Subject"] == "Claims pipeline FAILED"
    assert "DataQualityError" in alert["Message"] and "bad data" in alert["Message"]


def test_transient_glue_error_is_retried(monkeypatch):
    class ConcurrentRunsExceededException(Exception):
        pass

    calls = []
    attempts = {"n": 0}

    class Flaky(FakeJob):
        def run(self, spark, args):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConcurrentRunsExceededException("busy")
            return super().run(spark, args)

    monkeypatch.setattr(run_local.importlib, "import_module", lambda name: Flaky(calls))
    monkeypatch.setattr(run_local.time, "sleep", lambda s: None)
    machine = LocalStateMachine(load_definition(), None, "lake", "landing", lambda m: None)
    machine.run({"run_date": "2026-10-21"})
    assert attempts["n"] == 7                                        # 1 failure + 6 jobs


def test_intrinsic_functions():
    data, ctx = {"d": "2026-10-21", "error": {"Error": "E", "Cause": "C"}}, {"Execution": {"StartTime": "2026-10-22T01:02:03Z"}}
    assert evaluate("States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 0)", data, ctx) == "2026-10-22"
    assert evaluate("States.Format('run {} failed: {} / {}', $.d, $.error.Error, $.error.Cause)", data, ctx) == \
        "run 2026-10-21 failed: E / C"
