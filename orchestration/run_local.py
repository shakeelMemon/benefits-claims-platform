"""Run the pipeline locally by walking the real Step Functions definition.

This reads orchestration/claims_pipeline.asl.json, the same file Terraform deploys to AWS,
and executes it state by state: Choice, Pass, Task (Glue job or SNS), Parallel, Succeed, Fail,
with Retry and Catch. Each Glue task runs the matching job's run() in a shared local Spark
session, so the local run follows exactly the path AWS would.

Usage (from the project folder):
    python orchestration/run_local.py --run-date 2026-10-20 --with-stream
    python orchestration/run_local.py --run-date 2026-10-22     # no files for that day: shows the failure path
"""

import argparse
import importlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "glue_jobs"))

from common.runtime import local_spark  # noqa: E402

JOB_PREFIX = "claims"
JOB_MODULES = {
    f"{JOB_PREFIX}-bronze-batch": "job_bronze_batch",
    f"{JOB_PREFIX}-silver": "job_silver",
    f"{JOB_PREFIX}-gold-dim-member": "job_gold_dim_member",
    f"{JOB_PREFIX}-gold-dimensions": "job_gold_dimensions",
    f"{JOB_PREFIX}-gold-facts": "job_gold_facts",
    f"{JOB_PREFIX}-data-quality": "job_data_quality",
}


class StateMachineFailed(Exception):
    pass


# ---------- tiny JSONPath and intrinsic-function support (what this definition uses) ----------

def get_path(path: str, data: dict, context: dict):
    source, rest = (context, path[3:]) if path.startswith("$$.") else (data, path[2:])
    value = source
    for part in [p for p in rest.split(".") if p]:
        if not isinstance(value, dict) or part not in value:
            raise KeyError(path)
        value = value[part]
    return value


def has_path(path, data, context):
    try:
        get_path(path, data, context)
        return True
    except KeyError:
        return False


def split_args(text: str):
    parts, depth, quote, cur = [], 0, False, ""
    for ch in text:
        if ch == "'":
            quote = not quote
        if not quote and ch == "(":
            depth += 1
        if not quote and ch == ")":
            depth -= 1
        if ch == "," and depth == 0 and not quote:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    parts.append(cur.strip())
    return parts


def evaluate(expr: str, data, context):
    expr = expr.strip()
    if expr.startswith("'"):
        return expr[1:-1]
    if expr.startswith("$"):
        return get_path(expr, data, context)
    if re.fullmatch(r"-?\d+", expr):
        return int(expr)
    m = re.match(r"^(States\.\w+)\((.*)\)$", expr, flags=re.S)
    if not m:
        raise ValueError(f"Unsupported expression: {expr}")
    fn, args = m.group(1), [evaluate(a, data, context) for a in split_args(m.group(2))]
    if fn == "States.Format":
        template, values = args[0], [str(v) for v in args[1:]]
        for v in values:
            template = template.replace("{}", v, 1)
        return template
    if fn == "States.StringSplit":
        return args[0].split(args[1])
    if fn == "States.ArrayGetItem":
        return args[0][int(args[1])]
    raise ValueError(f"Unsupported intrinsic: {fn}")


def resolve(params, data, context):
    if isinstance(params, dict):
        out = {}
        for k, v in params.items():
            if k.endswith(".$"):
                out[k[:-2]] = evaluate(v, data, context)
            else:
                out[k] = resolve(v, data, context)
        return out
    return params


# ---------- the interpreter ----------

class LocalStateMachine:
    def __init__(self, definition: dict, spark, lake_root: str, landing_root: str, log):
        self.definition, self.spark, self.log = definition, spark, log
        self.lake_root, self.landing_root = lake_root, landing_root
        self.context = {"Execution": {"StartTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")}}
        self.notifications = []

    def run(self, data: dict):
        return self._run_states(self.definition, data)

    def _run_states(self, machine, data):
        name = machine["StartAt"]
        while True:
            state = machine["States"][name]
            kind = state["Type"]
            if kind == "Choice":
                name = self._choice(state, data)
                continue
            if kind == "Succeed":
                self.log(f"[{name}] Succeed")
                return data
            if kind == "Fail":
                self.log(f"[{name}] Fail: {state.get('Error')}")
                raise StateMachineFailed(state.get("Error"))
            if kind == "Pass":
                data = resolve(state["Parameters"], data, self.context) if "Parameters" in state else data
                self.log(f"[{name}] Pass -> {json.dumps(data)}")
            elif kind in ("Task", "Parallel"):
                try:
                    result = self._with_retry(name, state, data)
                    data = self._apply_result(state, data, result)
                except Exception as exc:  # noqa: BLE001 - Catch decides
                    catcher = self._catcher(state, exc)
                    if not catcher:
                        raise
                    error = {"Error": type(exc).__name__, "Cause": str(exc)}
                    self.log(f"[{name}] caught {error['Error']}: {error['Cause']}")
                    data = self._apply_result(catcher, data, error)
                    name = catcher["Next"]
                    continue
            if state.get("End"):
                return data
            name = state["Next"]

    def _choice(self, state, data):
        for rule in state["Choices"]:
            if "IsPresent" in rule and has_path(rule["Variable"], data, self.context) == rule["IsPresent"]:
                return rule["Next"]
        return state["Default"]

    def _with_retry(self, name, state, data):
        attempt = 0
        while True:
            try:
                return self._execute(name, state, data)
            except Exception as exc:  # noqa: BLE001
                retrier = next((r for r in state.get("Retry", []) if self._matches(r["ErrorEquals"], exc)), None)
                if not retrier or attempt >= retrier.get("MaxAttempts", 3):
                    raise
                wait = min(retrier.get("IntervalSeconds", 1) * retrier.get("BackoffRate", 2) ** attempt, 2)
                attempt += 1
                self.log(f"[{name}] retry {attempt} after {type(exc).__name__}")
                time.sleep(wait)                 # shortened locally

    @staticmethod
    def _matches(errors, exc):
        # Locally a failed job raises a Python exception; match it to "Glue.<Name>" by its name
        name = type(exc).__name__
        return ("States.ALL" in errors or "States.TaskFailed" in errors
                or any(e == name or e.split(".")[-1] == name for e in errors))

    def _catcher(self, state, exc):
        return next((c for c in state.get("Catch", []) if self._matches(c["ErrorEquals"], exc)), None)

    @staticmethod
    def _apply_result(state, data, result):
        path = state.get("ResultPath", "$")
        if path is None:
            return data
        if path == "$":
            return result
        out = dict(data)
        out[path[2:]] = result
        return out

    def _execute(self, name, state, data):
        if state["Type"] == "Parallel":
            self.log(f"[{name}] Parallel: {len(state['Branches'])} branches")
            return [self._run_states(branch, data) for branch in state["Branches"]]
        resource, params = state["Resource"], resolve(state.get("Parameters", {}), data, self.context)
        if "glue:startJobRun" in resource:
            job = params["JobName"]
            args = {k.lstrip("-"): v for k, v in params.get("Arguments", {}).items()}
            args.update({"lake_root": self.lake_root, "landing_root": self.landing_root})
            module = importlib.import_module(JOB_MODULES[job])
            defaults = dict(module.DEFAULTS)
            defaults.update(args)
            self.log(f"[{name}] Glue job {job} ...")
            started = time.time()
            result = module.run(self.spark, defaults)
            self.log(f"[{name}]   done in {time.time() - started:.1f}s: {result}")
            return result
        if "sns:publish" in resource:
            self.notifications.append(params)
            self.log(f"[{name}] SNS -> {params['Subject']}: {params['Message']}")
            return {"MessageId": "local"}
        raise ValueError(f"Unsupported resource {resource}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", help="YYYY-MM-DD; omit to use today, like a scheduled run")
    ap.add_argument("--lake-root", default=str(ROOT / "local" / "lake"))
    ap.add_argument("--landing-root", default=str(ROOT / "local" / "landing"))
    ap.add_argument("--with-stream", action="store_true", help="first ingest new claim events from the stream")
    ap.add_argument("--stream-source", default="files", choices=["files", "kafka"])
    args = ap.parse_args()

    spark = local_spark(kafka=args.stream_source == "kafka")
    if args.with_stream:
        import job_stream_claims_to_bronze as stream
        result = stream.run(spark, {**stream.DEFAULTS, "lake_root": args.lake_root, "source": args.stream_source,
                                    "stream_input": str(ROOT / "local" / "stream" / "incoming")})
        print(f"[StreamIngest] {result}")

    definition = json.loads((ROOT / "orchestration" / "claims_pipeline.asl.json").read_text()
                            .replace("${job_prefix}", JOB_PREFIX)
                            .replace("${sns_topic_arn}", "arn:aws:sns:local:000000000000:claims-pipeline-alerts"))
    machine = LocalStateMachine(definition, spark, args.lake_root, args.landing_root, print)
    payload = {"run_date": args.run_date} if args.run_date else {}
    try:
        machine.run(payload)
        print("\nPIPELINE SUCCEEDED")
    except StateMachineFailed:
        print("\nPIPELINE FAILED (alert sent, see the NotifyFailure line above)")
        sys.exit(1)


if __name__ == "__main__":
    main()
