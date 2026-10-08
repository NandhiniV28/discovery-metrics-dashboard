"""
Discovery Dashboard data pipeline — Communications Services zone.

Pulls TDI (Jira project TDI, issue type Feature) data for a given Zone,
computes 3S-cycle bucketed metrics, and returns a single JSON-able dict
with raw per-issue records so the dashboard can filter/aggregate client-side.

Credentials: reads JIRA_EMAIL / JIRA_API_TOKEN from the environment first
(used in GitHub Actions, via repo secrets), falling back to
~/.config/jira/token + a hardcoded email for local runs. Never logs or
returns the token itself.
"""
import json
import math
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

JIRA_BASE = "https://talkdesk.atlassian.net"
DEFAULT_EMAIL = "nandhini.venkatesan@talkdesk.com"
ZONE = "Communications Services"

# Official 3S calendar anchor: cycle "260102" ends 2026-01-02. Jira changelog
# timestamps carry a local (-06:00/-07:00) offset, not UTC - anchoring at UTC
# midnight meant an event late in the evening on the official end date (e.g.
# 2026-09-11T19:38-06:00) was already past midnight UTC on the 12th and got
# pushed into the *next* cycle. Anchor at end-of-day in that local offset
# instead so the whole calendar day of the end date belongs to its cycle.
CYCLE_TZ = timezone(timedelta(hours=-6))
ANCHOR_END = datetime(2026, 1, 2, 23, 59, 59, 999999, tzinfo=CYCLE_TZ)
STEP = timedelta(days=42)
CYCLE_LEN = timedelta(days=39)
WINDOW_CYCLES = 6

FIELDS_COMMON = (
    "summary,status,fixVersions,assignee,reporter,parent,"
    "customfield_15092,"  # Talkdesk Product
    "customfield_13360,"  # TDI Category
    "customfield_15470,"  # R&D Product Area
    "customfield_15467,"  # Allocation in Days
    "customfield_10007,"  # Sprint
    "customfield_19499,"  # Tech Owner
    "customfield_19500,"  # Design Owner
    "customfield_19501"   # Next Milestone Date
)


def _format_sprints(sprint_field):
    names = [s["name"] for s in (sprint_field or []) if s.get("name")]
    return ", ".join(names) if names else None


def _token():
    env_token = os.environ.get("JIRA_API_TOKEN")
    if env_token:
        return env_token.strip()
    with open(os.path.expanduser("~/.config/jira/token")) as f:
        return f.read().strip()


def _email():
    return os.environ.get("JIRA_EMAIL", DEFAULT_EMAIL)


def _auth():
    return f"{_email()}:{_token()}"


def _curl_get(url, params):
    cmd = ["curl", "-s", "-u", _auth(), "-H", "Accept: application/json", "-G"]
    for k, v in params.items():
        cmd += ["--data-urlencode", f"{k}={v}"]
    cmd.append(url)
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout
    return json.loads(out)


def _search_all(jql, fields, expand=None):
    base = f"{JIRA_BASE}/rest/api/3/search/jql"
    all_issues = []
    next_token = None
    while True:
        params = {"jql": jql, "maxResults": "100", "fields": fields}
        if expand:
            params["expand"] = expand
        if next_token:
            params["nextPageToken"] = next_token
        d = _curl_get(base, params)
        if "issues" not in d:
            raise RuntimeError(f"Jira search failed: {d}")
        all_issues.extend(d["issues"])
        if d.get("isLast", True) or not d.get("nextPageToken"):
            break
        next_token = d["nextPageToken"]
    return all_issues


def _fetch_full_changelog(key):
    url = f"{JIRA_BASE}/rest/api/3/issue/{key}/changelog"
    histories = []
    start_at = 0
    while True:
        d = _curl_get(url, {"startAt": str(start_at), "maxResults": "100"})
        histories.extend(d.get("values", []))
        if d.get("isLast", True):
            break
        start_at += len(d.get("values", []))
    return histories


def _extract_field(f, key):
    v = f.get(key)
    if v is None:
        return None
    if isinstance(v, list):
        return v[0].get("value") if v else None
    if isinstance(v, dict):
        return v.get("value")
    return v


def _issue_record(i):
    f = i["fields"]
    return {
        "key": i["key"],
        "summary": f.get("summary"),
        "status": f["status"]["name"],
        "fix_versions": [fv["name"] for fv in (f.get("fixVersions") or [])],
        "assignee": (f.get("assignee") or {}).get("displayName"),
        "reporter": (f.get("reporter") or {}).get("displayName"),
        "product": _extract_field(f, "customfield_15092"),
        "tdi_category": _extract_field(f, "customfield_13360"),
        "rd_product_area": _extract_field(f, "customfield_15470"),
        "sprint": _format_sprints(f.get("customfield_10007")),
        "tech_owner": (f.get("customfield_19499") or {}).get("displayName"),
        "design_owner": (f.get("customfield_19500") or {}).get("displayName"),
        "milestone_date": f.get("customfield_19501"),
        "url": f"{JIRA_BASE}/browse/{i['key']}",
    }


def cycle_index_for(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta_days = (dt - ANCHOR_END).total_seconds() / 86400
    return math.ceil(delta_days / 42)


def cycle_bounds(n):
    end = ANCHOR_END + STEP * n
    start = end - CYCLE_LEN
    return start, end, end.strftime("%y%m%d")


def _parse_dt(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")


def run(zone=ZONE, window_cycles=WINDOW_CYCLES, progress=None):
    """Run the full pipeline. `progress(msg)` is called with status strings."""
    def log(msg):
        if progress:
            progress(msg)

    # "Active" TDI = has an assignee and a target quarter (fixVersion). Confirmed
    # against the GitHub release dashboard: of 67 raw "In Discovery" TDIs in
    # Communications Services, exactly 52 have both an assignee and a fixVersion
    # set (the other 15 have neither/one), matching the count shown there.
    zone_jql = (
        f'project = TDI and issuetype = Feature and "Zone[Select List (multiple choices)]" IN ("{zone}") '
        f'and assignee is not EMPTY and fixVersion is not EMPTY'
    )

    log("Fetching all zone TDIs...")
    all_tdis = _search_all(zone_jql + " and status != Backlog", FIELDS_COMMON, expand="changelog")

    log("Fetching current In Discovery snapshot...")
    current_in_discovery_raw = _search_all(zone_jql + ' and status = "In Discovery"', FIELDS_COMMON)
    current_in_discovery = [_issue_record(i) for i in current_in_discovery_raw]

    log(f"Extracting changelogs for {len(all_tdis)} issues...")
    truncated_keys = []
    issue_status_events = {}
    issue_meta = {}
    for i in all_tdis:
        key = i["key"]
        issue_meta[key] = _issue_record(i)
        cl = i.get("changelog", {})
        histories = cl.get("histories", [])
        if cl.get("total", 0) > len(histories):
            truncated_keys.append(key)
            continue
        events = []
        for h in histories:
            for it in h.get("items", []):
                if it.get("field") == "status":
                    events.append((h["created"], it.get("fromString"), it.get("toString")))
        issue_status_events[key] = events

    if truncated_keys:
        log(f"Refetching {len(truncated_keys)} truncated changelogs in parallel...")
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_fetch_full_changelog, k): k for k in truncated_keys}
            for fut in as_completed(futures):
                k = futures[fut]
                histories = fut.result()
                events = []
                for h in histories:
                    for it in h.get("items", []):
                        if it.get("field") == "status":
                            events.append((h["created"], it.get("fromString"), it.get("toString")))
                issue_status_events[k] = events

    log("Computing Backlog/In Discovery -> Ready to Plan transitions...")
    transitions = []
    for key, events in issue_status_events.items():
        events_sorted = sorted(events, key=lambda e: _parse_dt(e[0]))
        last_entry = None
        for idx, (created, frm, to) in enumerate(events_sorted):
            dt = _parse_dt(created)
            if to == "In Discovery":
                last_entry = dt
            # Count both the normal In Discovery -> Ready to Plan path and a
            # TDI that skipped discovery entirely (Backlog -> Ready to Plan
            # directly). The latter has no discovery entry, so duration_days
            # stays None for it - there's nothing to measure.
            if to == "Ready to Plan" and frm in ("In Discovery", "Backlog"):
                # Skip a "completion" that gets reversed right back to
                # Backlog/In Discovery as its very next status change - that's
                # a correction/bounce, not a genuine completion (e.g. someone
                # moved it forward, realized it wasn't ready, and reverted it
                # within hours). Without this, the same TDI shows up multiple
                # times with inconsistent dates/durations for what is really
                # one still-unresolved discovery.
                next_to = events_sorted[idx + 1][2] if idx + 1 < len(events_sorted) else None
                if next_to in ("In Discovery", "Backlog"):
                    continue
                duration = (dt - last_entry).total_seconds() / 86400 if last_entry else None
                transitions.append({
                    "id": f"{key}|{dt.isoformat()}",
                    "key": key,
                    **issue_meta[key],
                    "exit_time": dt.isoformat(),
                    "entry_time": last_entry.isoformat() if last_entry else None,
                    "duration_days": duration,
                })

    now = datetime.now(timezone.utc)
    current_n = cycle_index_for(now)
    current_start, current_end, current_label = cycle_bounds(current_n)

    log("Fetching DISCOVERY epics (completed + ongoing) and allocation...")
    # Search broadly (not just epics of already-transitioned TDIs) so we also
    # catch epics still in progress under TDIs that haven't moved to Ready to
    # Plan yet - needed to compute "ongoing" discovery work below. Exclude the
    # Design team's project (DES) - their discovery epics don't count toward
    # this zone's discovery epic count or allocation totals.
    epic_jql = 'issuetype = Epic and summary ~ "discovery" and project != DES'
    epic_issues = _search_all(epic_jql, "summary,status,customfield_15467,customfield_10007,parent")

    zone_tdi_keys = set(issue_meta.keys())

    def _sprint_overlaps_current(sprints):
        for s in sprints or []:
            sd, ed = s.get("startDate"), s.get("endDate")
            if not sd or not ed:
                continue
            s_start = datetime.fromisoformat(sd.replace("Z", "+00:00"))
            s_end = datetime.fromisoformat(ed.replace("Z", "+00:00"))
            if s_start < current_end and s_end > current_start:
                return True
        return False

    alloc_by_tdi = {}
    epics_by_tdi = {}
    ongoing_by_tdi = {}
    for i in epic_issues:
        if i["key"].startswith("DES-"):
            continue  # belt-and-suspenders in case the JQL exclusion doesn't match
        f = i["fields"]
        parent_key = (f.get("parent") or {}).get("key")
        if parent_key not in zone_tdi_keys:
            continue  # not a Communications Services TDI in our (assignee+fixVersion-filtered) universe
        alloc = float(f["customfield_15467"]) if f.get("customfield_15467") else 0.0
        status_name = f["status"]["name"]
        epic_record = {
            "key": i["key"], "summary": f["summary"], "status": status_name,
            "alloc_days": alloc, "url": f"{JIRA_BASE}/browse/{i['key']}",
            "sprint": _format_sprints(f.get("customfield_10007")),
        }
        alloc_by_tdi[parent_key] = alloc_by_tdi.get(parent_key, 0.0) + alloc
        epics_by_tdi.setdefault(parent_key, []).append(epic_record)

        if status_name not in ("Done", "Closed") and _sprint_overlaps_current(f.get("customfield_10007")):
            ongoing_by_tdi.setdefault(parent_key, []).append(epic_record)

    for t in transitions:
        t["alloc_days"] = round(alloc_by_tdi.get(t["key"], 0.0), 2)
        t["discovery_epics"] = epics_by_tdi.get(t["key"], [])

    ongoing_discovery = []
    for tdi_key, epics in ongoing_by_tdi.items():
        ongoing_discovery.append({
            "key": tdi_key,
            **issue_meta[tdi_key],
            "alloc_days": round(sum(e["alloc_days"] for e in epics), 2),
            "discovery_epics": epics,
        })

    log("Bucketing into 3S cycles...")
    window_ns = list(range(current_n - window_cycles + 1, current_n + 1))
    cycles = []
    for n in window_ns:
        start, end, label = cycle_bounds(n)
        in_cycle = [t for t in transitions if cycle_index_for(_parse_dt(t["exit_time"])) == n]
        durations = [t["duration_days"] for t in in_cycle if t["duration_days"] is not None]
        cycles.append({
            "n": n,
            "label": label,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "in_progress": label == cycle_bounds(current_n)[2],
            "transition_ids": [t["id"] for t in in_cycle],
            "count": len(in_cycle),
            "alloc_sum": round(sum(t["alloc_days"] for t in in_cycle), 2),
            "avg_duration": round(sum(durations) / len(durations), 2) if durations else None,
        })

    windowed_ids = {tid for c in cycles for tid in c["transition_ids"]}
    for t in transitions:
        t["in_window"] = t["id"] in windowed_ids
        t["cycle_label"] = next((c["label"] for c in cycles if t["id"] in c["transition_ids"]), None)

    log("Done.")
    return {
        "generated_at": now.isoformat(),
        "zone": zone,
        "window_cycles": window_cycles,
        "current_in_discovery": current_in_discovery,
        "transitions": transitions,
        "cycles": cycles,
        "ongoing_discovery": {
            "cycle_label": current_label,
            "cycle_start": current_start.isoformat(),
            "cycle_end": current_end.isoformat(),
            "tdis": ongoing_discovery,
        },
        "all_time": {
            "total_transitions": len(transitions),
            "total_allocation_days": round(sum(alloc_by_tdi.values()), 2),
        },
    }


if __name__ == "__main__":
    data = run(progress=lambda m: print(m, flush=True))
    out_path = os.path.join(os.path.dirname(__file__), "data.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {out_path}")
    print(f"current_in_discovery: {len(data['current_in_discovery'])}")
    print(f"transitions (all-time): {data['all_time']['total_transitions']}")
    print(f"windowed transitions: {sum(1 for t in data['transitions'] if t['in_window'])}")
    dup_keys = len(data['transitions']) - len({t['key'] for t in data['transitions']})
    print(f"TDIs with multiple transitions (all-time): {dup_keys}")
