#!/usr/bin/env python3
"""Replay reviewed CodeQL dismissals that GitHub drops when line numbers shift.

An alert is dismissed only when its rule id, file path, and the *text* of the
line it points at all match an entry in the suppression list. Anchoring on the
sink text rather than the line number is the point: a shifted line is what
re-raises the alert, and a genuinely new sink in the same file will not match
an existing entry and stays open.

Exit code is 0 when every open alert is either dismissed or reported; the
report goes to the workflow step summary so unreviewed alerts stay visible.
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"
REASON_LIMIT = 280  # GitHub's cap on dismissed_comment.


def call(method, path, token, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{API}{path}", data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read() or b"null")


def open_alerts(repo, token, ref):
    out, page = [], 1
    while True:
        batch = call(
            "GET",
            f"/repos/{repo}/code-scanning/alerts"
            f"?state=open&ref={ref}&per_page=100&page={page}",
            token,
        )
        if not batch:
            return out
        out.extend(batch)
        page += 1


def source_line(path, line_no):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f, 1):
                if i == line_no:
                    return line.rstrip("\n")
    except OSError:
        return None
    return None


def match(alert, rules):
    loc = alert["most_recent_instance"]["location"]
    path, line_no = loc["path"], loc["start_line"]
    text = source_line(path, line_no)
    if text is None:
        return None
    for rule in rules:
        if rule["rule"] != alert["rule"]["id"] or rule["path"] != path:
            continue
        if re.search(rule["sink"], text):
            return rule
    return None


def main():
    token = os.environ["GITHUB_TOKEN"]
    repo = os.environ["GITHUB_REPOSITORY"]
    ref = os.environ.get("TARGET_REF", "refs/heads/main")
    dry_run = os.environ.get("DRY_RUN") == "true"

    with open(".github/codeql/suppressions.json", encoding="utf-8") as f:
        rules = json.load(f)["suppressions"]

    for rule in rules:
        if len(rule["reason"]) > REASON_LIMIT:
            sys.exit(
                f"suppressions.json: reason for {rule['rule']} on {rule['path']} is "
                f"{len(rule['reason'])} chars; the API rejects anything over {REASON_LIMIT}"
            )
        re.compile(rule["sink"])

    dismissed, unmatched = [], []
    for alert in open_alerts(repo, token, ref):
        loc = alert["most_recent_instance"]["location"]
        where = f"{alert['rule']['id']} {loc['path']}:{loc['start_line']}"
        rule = match(alert, rules)
        if rule is None:
            unmatched.append(f"#{alert['number']} {where}")
            continue
        if not dry_run:
            call(
                "PATCH",
                f"/repos/{repo}/code-scanning/alerts/{alert['number']}",
                token,
                {
                    "state": "dismissed",
                    "dismissed_reason": "false positive",
                    "dismissed_comment": rule["reason"],
                },
            )
        dismissed.append(f"#{alert['number']} {where}")

    report = ["## CodeQL suppression replay", ""]
    verb = "Would dismiss" if dry_run else "Dismissed"
    report.append(f"{verb} {len(dismissed)} re-raised alert(s):")
    report += [f"- `{d}`" for d in dismissed] or ["- none"]
    report += ["", f"Open and unreviewed — {len(unmatched)} alert(s):"]
    report += [f"- `{u}`" for u in unmatched] or ["- none"]

    text = "\n".join(report)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as e:
        sys.exit(f"GitHub API {e.code}: {e.read().decode(errors='replace')}")
