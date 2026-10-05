#!/usr/bin/env python3
"""Recount docs/COVERAGE.md from its own tables and check what it cites.

    python3 docs/check_coverage.py          recount, check Bruno and DAG references
    python3 docs/check_coverage.py --live   also check shim routes against the running shim
    python3 docs/check_coverage.py --check  change nothing; exit 1 if the summary is out of date
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOC = os.path.join(ROOT, "docs", "COVERAGE.md")
VERDICTS = ["COVERED", "STAND-IN", "PARTIAL", "NOT COVERED"]
EXPECTED = {"REP": 5, "ORC": 6, "ING": 6, "EXT": 7, "RPT": 7, "ALR": 3, "DSH": 2, "NFR": 13, "SEC": 12}
BEGIN, END = "<!-- summary:begin -->", "<!-- summary:end -->"
ROW = re.compile(r"^\| ((?:REP|ORC|ING|EXT|RPT|ALR|DSH|NFR|SEC)-\d\d) \|")


def rows(text: str) -> list[tuple[str, str, str, str]]:
    out = []
    for line in text.splitlines():
        if not ROW.match(line):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 6:
            sys.exit(f"{cells[0]}: expected 6 cells, found {len(cells)}")
        verdict = cells[4].strip("*")
        if verdict not in VERDICTS:
            sys.exit(f"{cells[0]}: unknown verdict {cells[4]!r}")
        out.append((cells[0], cells[1], cells[3], verdict))
    return out


def dag_tasks() -> set[str]:
    found = set()
    for path in glob.glob(os.path.join(ROOT, "infra", "airflow", "dags", "nfl_*.py")):
        dag = os.path.splitext(os.path.basename(path))[0]
        for task in re.findall(r'task_id="([a-z_]+)"', open(path).read()):
            found.add(f"{dag}.{task}")
    return found


def expand(text: str) -> list[str]:
    """Every `NN-folder/NN` cited, with `a` to `b` ranges in one folder expanded."""
    refs: list[str] = []
    for folder, lo, hi in re.findall(r"`(\d\d-[a-z]+)/(\d\d)` to `\1/(\d\d)`", text):
        present = sorted(
            os.path.basename(p)[:2] for p in glob.glob(os.path.join(ROOT, "bruno", folder, "[0-9][0-9]-*.bru"))
        )
        refs += [f"{folder}/{n}" for n in present if lo <= n <= hi]
    refs += [f"{f}/{n}" for f, n in re.findall(r"`(\d\d-[a-z]+)/(\d\d)`", text)]
    return refs


def main() -> None:
    live = "--live" in sys.argv
    check_only = "--check" in sys.argv
    text = open(DOC).read()
    table = rows(text)
    problems: list[str] = []

    ids = [r[0] for r in table]
    for prefix, count in EXPECTED.items():
        want = [f"{prefix}-{i:02d}" for i in range(1, count + 1)]
        have = [i for i in ids if i.startswith(prefix)]
        if have != want:
            problems.append(f"{prefix}: expected {want}, found {have}")

    tasks = dag_tasks()
    routes: set[str] | None = None
    if live:
        url = os.environ.get("NFL_SHIM_URL", "http://localhost:8096") + "/openapi.json"
        routes = set(json.load(urllib.request.urlopen(url, timeout=10))["paths"])

    cited_requests: set[str] = set()
    for rid, _pri, cited, verdict in table:
        for ref in expand(cited):
            folder, seq = ref.split("/")
            hits = glob.glob(os.path.join(ROOT, "bruno", folder, f"{seq}-*.bru"))
            if len(hits) != 1:
                problems.append(f"{rid}: `{ref}` resolves to {len(hits)} files")
            cited_requests.add(ref)
        for task in re.findall(r"`(nfl_(?:ingestion|extraction|reporting)\.[a-z_]+)`", cited):
            if task not in tasks:
                problems.append(f"{rid}: task `{task}` is not defined in infra/airflow/dags")
        for route in re.findall(r"`(/checks/[a-z0-9/-]+)`", cited):
            if routes is not None and route not in routes:
                problems.append(f"{rid}: route `{route}` is not served by the shim")
        has_request = bool(expand(cited))
        if verdict in ("COVERED", "STAND-IN") and not has_request:
            problems.append(f"{rid}: {verdict} but no Bruno request is cited")
        if verdict == "NOT COVERED" and has_request:
            problems.append(f"{rid}: NOT COVERED but a Bruno request is cited")

    counts = {v: sum(1 for r in table if r[3] == v) for v in VERDICTS}
    must = [r for r in table if r[1] == "Must"]
    must_counts = {v: sum(1 for r in must if r[3] == v) for v in VERDICTS}
    lines = [
        f"Requirement IDs in the specification: **{len(table)}**. "
        f"Bruno requests cited: **{len(cited_requests)}** distinct files.",
        "",
        "| Verdict | All 61 IDs | Of the " + str(len(must)) + " functional IDs with priority Must |",
        "|---|---|---|",
    ]
    lines += [f"| **{v}** | {counts[v]} | {must_counts[v]} |" for v in VERDICTS]
    lines += ["", "| Area | " + " | ".join(VERDICTS) + " |", "|---|" + "---|" * len(VERDICTS)]
    for prefix in EXPECTED:
        per = [sum(1 for r in table if r[0].startswith(prefix) and r[3] == v) for v in VERDICTS]
        lines.append(f"| {prefix} | " + " | ".join(str(n) for n in per) + " |")
    block = BEGIN + "\n" + "\n".join(lines) + "\n" + END

    if BEGIN not in text or END not in text:
        sys.exit("summary markers missing from docs/COVERAGE.md")
    new = text[: text.index(BEGIN)] + block + text[text.index(END) + len(END):]

    if problems:
        print("\n".join(problems))
        sys.exit(1)
    if new != text:
        if check_only:
            sys.exit("summary block is out of date: run python3 docs/check_coverage.py")
        open(DOC, "w").write(new)
    print(f"{len(table)} rows: " + ", ".join(f"{v} {counts[v]}" for v in VERDICTS))
    print("routes checked against the live shim" if live else "routes NOT checked (run with --live)")


if __name__ == "__main__":
    main()
