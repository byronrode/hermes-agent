"""Resource invalidations retained atomically by the existing cron save owner."""


def _payload(data):
    """Accept the same legacy job collections as load_jobs, without writing them."""
    if isinstance(data, list):
        return {"jobs": data}
    if not isinstance(data, dict):
        raise ValueError("Invalid cron resource store")
    result = dict(data)
    raw = result.get("jobs", [])
    if isinstance(raw, dict):
        raw = [dict(value, id=value.get("id") or key)
               for key, value in raw.items() if isinstance(value, dict)]
    if not isinstance(raw, list):
        raise ValueError("Invalid cron job collection")
    result["jobs"] = raw
    return result


def staged_changes(jobs_file, jobs):
    # The mutation owner calls this while holding its existing save lock.
    from cron.jobs import _parse_jobs_file
    from gateway.platforms.api_server_resource_events import change

    try:
        previous, _ = _parse_jobs_file(jobs_file) if jobs_file.exists() else ({}, False)
    except (OSError, ValueError):
        previous = {}
    previous = _payload(previous)
    old = {j["id"]: j for j in previous["jobs"] if isinstance(j, dict) and j.get("id")}
    current = {j["id"]: j for j in jobs if isinstance(j, dict) and j.get("id")}
    changes = list(previous.get("resource_changes", []))
    for identity in sorted(old.keys() | current.keys()):
        if old.get(identity) != current.get(identity):
            job = current.get(identity) or old[identity]
            changes.append(change("automations", identity, profile=job.get("profile") or "default",
                visible=not any(job.get(k) for k in
                    ("workdir", "script", "no_agent", "context_from", "monitor_script", "monitor_url"))))
    return changes[-512:]


def resource_changes():
    """Read retained changes under the owning profile's store lock."""
    from cron.jobs import _jobs_lock, _current_cron_store, _parse_jobs_file

    with _jobs_lock():
        path = _current_cron_store().jobs_file
        if not path.exists():
            return []
        data, _ = _parse_jobs_file(path)
        data = _payload(data)
        current = {j["id"]: j for j in data["jobs"] if isinstance(j, dict) and j.get("id")}
        changes = data.get("resource_changes", [])
        latest = {e["id"]: e for e in changes}
        return [e for e in changes if e.get("profile") in (None, "", "default")
                and e.get("visible") is True and latest[e["id"]].get("visible") is True
                and latest[e["id"]].get("profile") in (None, "", "default")
                and (e["id"] not in current or (
                    current[e["id"]].get("profile") in (None, "", "default")
                    and not any(current[e["id"]].get(k) for k in
                        ("workdir", "script", "no_agent", "context_from", "monitor_script", "monitor_url"))))]
