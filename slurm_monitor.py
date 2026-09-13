#!/usr/bin/env python3
"""Periodic Slurm job report -> Slack (webhook) or email (HTTPS API).

Reports, for every active job and every job that finished since the last
report: job id, submit time, a one-line summary, current status, estimated
or actual start, and estimated or actual finish.

Usage:
    slurm_monitor.py                 # build report and send it
    slurm_monitor.py --dry-run       # print the report, send nothing
    slurm_monitor.py --test          # send a short "it works" message
    slurm_monitor.py --install-cron  # schedule 08:00 and 20:00 Asia/Taipei
    slurm_monitor.py --uninstall-cron
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - python < 3.9
    ZoneInfo = None

HOME = os.path.expanduser("~")
CONFIG_PATH = os.environ.get(
    "SLURM_MONITOR_CONFIG", os.path.join(HOME, ".config/slurm-monitor/config.json")
)
STATE_DIR = os.path.join(HOME, ".local/state/slurm-monitor")
STATE_PATH = os.path.join(STATE_DIR, "state.json")
LOG_PATH = os.path.join(STATE_DIR, "monitor.log")
CACHE_PATH = os.path.join(STATE_DIR, "wait_cache.json")

DEFAULTS = {
    "channel": "slack",           # slack | email | stdout
    "slack_webhook_url": "",
    "email": {
        "provider": "resend",     # resend | sendgrid
        "api_key": "",
        "from": "",
        "to": "",
    },
    "user": "",                   # defaults to $USER
    "timezone": "Asia/Taipei",
    "finished_lookback_hours": 14,  # floor for "recently finished" window
    "max_finished": 15,
    "queue_context": True,          # add queue position + historical wait to PENDING jobs
    "wait_stats_days": 21,          # history window for the typical-wait figures
    "wait_stats_min_sample": 10,    # below this many samples, report no wait stats
}

TERMINAL_STATES = {
    "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY",
    "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED",
}
STATE_ICON = {
    "RUNNING": ":large_green_circle:",
    "PENDING": ":hourglass_flowing_sand:",
    "COMPLETED": ":white_check_mark:",
    "FAILED": ":x:",
    "TIMEOUT": ":alarm_clock:",
    "CANCELLED": ":black_square_for_stop:",
    "OUT_OF_MEMORY": ":rotating_light:",
    "NODE_FAIL": ":rotating_light:",
    "PREEMPTED": ":leftwards_arrow_with_hook:",
    "SUSPENDED": ":double_vertical_bar:",
}
STATE_ORDER = ["RUNNING", "PENDING", "SUSPENDED", "COMPLETING", "CONFIGURING"]


# --------------------------------------------------------------------------
# config / state
# --------------------------------------------------------------------------
def deep_merge(base, override):
    out = dict(base)
    for key, val in (override or {}).items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def load_config():
    cfg = dict(DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as fh:
                cfg = deep_merge(cfg, json.load(fh))
        except ValueError as exc:
            raise SystemExit("Config file %s is not valid JSON: %s" % (CONFIG_PATH, exc))
    # environment always wins, so secrets can stay out of the file
    if os.environ.get("SLACK_WEBHOOK_URL"):
        cfg["slack_webhook_url"] = os.environ["SLACK_WEBHOOK_URL"]
    if os.environ.get("SLURM_MONITOR_EMAIL_KEY"):
        cfg["email"]["api_key"] = os.environ["SLURM_MONITOR_EMAIL_KEY"]
    if not cfg.get("user"):
        cfg["user"] = os.environ.get("USER") or os.environ.get("LOGNAME") or ""
    return cfg


def load_state():
    try:
        with open(STATE_PATH) as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_state(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh, indent=2)
    os.replace(tmp, STATE_PATH)


def log(msg):
    os.makedirs(STATE_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a") as fh:
        fh.write("%s %s\n" % (stamp, msg))


# --------------------------------------------------------------------------
# time helpers
# --------------------------------------------------------------------------
def tzinfo(cfg):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(cfg.get("timezone") or "Asia/Taipei")
        except Exception:
            pass
    return None


def num(field):
    """Slurm JSON wraps scalars as {set, infinite, number}."""
    if isinstance(field, dict):
        if not field.get("set", True) or field.get("infinite"):
            return 0
        return field.get("number") or 0
    return field or 0


def fmt_ts(epoch, tz, with_year=False):
    if not epoch:
        return None
    dt = datetime.fromtimestamp(epoch, tz)
    return dt.strftime("%a %d %b %Y %H:%M" if with_year else "%a %d %b %H:%M")


def fmt_dur(seconds):
    seconds = int(seconds)
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins = rem // 60
    if days:
        return "%s%dd %dh" % (sign, days, hours)
    if hours:
        return "%s%dh %dm" % (sign, hours, mins)
    return "%s%dm" % (sign, mins)


def parse_slurm_duration(text):
    """[D-]HH:MM:SS or MM:SS -> seconds. 0 for UNLIMITED/INVALID/blank."""
    text = (text or "").strip()
    if not text or text in ("UNLIMITED", "INVALID", "NOT_SET", "Partition_Limit", "N/A"):
        return 0
    days = 0
    if "-" in text:
        head, _, text = text.partition("-")
        try:
            days = int(head)
        except ValueError:
            return 0
    parts = text.split(":")
    try:
        nums = [int(x) for x in parts]
    except ValueError:
        return 0
    if len(nums) == 3:
        return days * 86400 + nums[0] * 3600 + nums[1] * 60 + nums[2]
    if len(nums) == 2:
        return days * 86400 + nums[0] * 60 + nums[1]
    if len(nums) == 1:
        return days * 86400 + nums[0] * 60
    return 0


def fmt_hours(hours):
    if hours < 1:
        return "%dm" % int(round(hours * 60))
    if hours < 10:
        return "%.1fh" % hours
    if hours < 48:
        return "%dh" % int(round(hours))
    return "%.1fd" % (hours / 24.0)


def size_bucket(nodes):
    nodes = int(nodes or 1)
    if nodes <= 1:
        return "1 node"
    if nodes <= 4:
        return "2-4 node"
    if nodes <= 8:
        return "5-8 node"
    return "9+ node"


def percentile(sorted_vals, frac):
    if not sorted_vals:
        return 0
    idx = int(round(frac * (len(sorted_vals) - 1)))
    return sorted_vals[max(0, min(idx, len(sorted_vals) - 1))]


def rel(epoch, now):
    if not epoch:
        return ""
    delta = epoch - now
    return "in %s" % fmt_dur(delta) if delta >= 0 else "%s ago" % fmt_dur(-delta)


# --------------------------------------------------------------------------
# slurm queries
# --------------------------------------------------------------------------
def run_json(cmd):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(
            "%s failed (%d): %s"
            % (" ".join(cmd), proc.returncode, proc.stderr.decode()[:400])
        )
    return json.loads(proc.stdout.decode() or "{}")


def gpus_from_tres(tres):
    for part in (tres or "").split(","):
        if part.startswith("gres/gpu="):
            return part.split("=", 1)[1]
    return None


def state_of(job_state):
    if isinstance(job_state, list):
        return job_state[0] if job_state else "UNKNOWN"
    return job_state or "UNKNOWN"


def fetch_active(user):
    data = run_json(["squeue", "-u", user, "--json"])
    jobs = []
    for j in data.get("jobs", []):
        limit_min = num(j.get("time_limit"))
        start = num(j.get("start_time"))
        end = num(j.get("end_time"))
        state = state_of(j.get("job_state"))
        tres = j.get("tres_alloc_str") or j.get("tres_req_str") or ""
        jobs.append({
            "job_id": j.get("job_id"),
            "name": j.get("name") or "",
            "state": state,
            "reason": (j.get("state_reason") or "").strip(),
            "partition": j.get("partition") or "",
            "priority": num(j.get("priority")),
            "nodes": num(j.get("node_count")),
            "nodelist": j.get("nodes") or "",
            "gpus": gpus_from_tres(tres),
            "limit_min": limit_min,
            "submit": num(j.get("submit_time")),
            "start": start,
            "end": end,
            "workdir": j.get("current_working_directory") or "",
            "command": j.get("command") or j.get("submit_line") or "",
            "stdout": j.get("stdout_expanded") or j.get("standard_output") or "",
            "finished": False,
        })
    return jobs


def fetch_finished(user, since_epoch, tz):
    start_arg = datetime.fromtimestamp(since_epoch, tz).strftime("%Y-%m-%dT%H:%M:%S")
    data = run_json([
        "sacct", "-u", user, "-X", "--json", "-S", start_arg,
    ])
    jobs = []
    for j in data.get("jobs", []):
        st = j.get("state") or {}
        state = state_of(st.get("current"))
        if state not in TERMINAL_STATES:
            continue
        t = j.get("time") or {}
        end = num(t.get("end"))
        if not end or end < since_epoch:
            continue
        tres = j.get("tres") or {}
        tres_list = tres.get("allocated") or tres.get("requested") or []
        gpus = None
        nodes = 0
        for item in tres_list:
            if item.get("type") == "gres" and item.get("name") == "gpu":
                gpus = str(item.get("count"))
            if item.get("type") == "node":
                nodes = item.get("count") or 0
        exit_code = j.get("exit_code") or {}
        rc = num((exit_code.get("return_code") or {}))
        jobs.append({
            "job_id": j.get("job_id"),
            "name": j.get("name") or "",
            "state": state,
            "reason": "exit code %s" % rc if state == "FAILED" else "",
            "partition": j.get("partition") or "",
            "nodes": nodes,
            "nodelist": j.get("nodes") or "",
            "gpus": gpus,
            "limit_min": num(t.get("limit")),
            "submit": num(t.get("submission")),
            "start": num(t.get("start")),
            "end": end,
            "elapsed": t.get("elapsed") or 0,
            "workdir": j.get("working_directory") or "",
            "command": j.get("submit_line") or j.get("script") or "",
            "stdout": j.get("stdout_expanded") or "",
            "finished": True,
        })
    jobs.sort(key=lambda x: x["end"], reverse=True)
    return jobs


class QueueContext(object):
    """Queue position and historical wait times, both measured, never modelled.

    Position ranks the partition's pending jobs by *priority*, not arrival order:
    Slurm is multifactor + backfill, so submission order means little.

    Typical wait reports the observed distribution of past queue waits for jobs of
    a similar size on the same partition. It is deliberately a median and a p90
    rather than a point estimate -- on a fairshare cluster the spread between them
    is usually more than an order of magnitude, and a single number would imply a
    confidence the scheduler does not have.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.days = cfg.get("wait_stats_days", 21)
        self.min_sample = cfg.get("wait_stats_min_sample", 10)
        self._queues = {}
        self._waits = None

    @staticmethod
    def _first_partition(name):
        return (name or "").split(",")[0].strip()

    # -- queue position ---------------------------------------------------
    def _queue(self, partition):
        if partition not in self._queues:
            rows = []
            try:
                proc = subprocess.run(
                    ["squeue", "-p", partition, "-h", "-t", "PD", "-o", "%Q|%D|%l"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
                if proc.returncode == 0:
                    for line in proc.stdout.decode().splitlines():
                        bits = line.split("|")
                        if len(bits) < 3:
                            continue
                        try:
                            prio = int(bits[0])
                            nodes = int(bits[1])
                        except ValueError:
                            continue
                        rows.append((prio, nodes, parse_slurm_duration(bits[2])))
            except Exception as exc:
                log("queue position lookup failed for %s: %s" % (partition, exc))
            self._queues[partition] = rows
        return self._queues[partition]

    def position(self, job):
        partition = self._first_partition(job.get("partition"))
        rows = self._queue(partition)
        if not rows:
            return None
        prio = job.get("priority") or 0
        ahead = [r for r in rows if r[0] > prio]
        node_hours = sum(r[1] * r[2] for r in ahead) / 3600.0
        return {
            "rank": len(ahead) + 1,
            "total": len(rows),
            "ahead": len(ahead),
            "node_hours": node_hours,
        }

    # -- historical wait --------------------------------------------------
    def _load_cache(self):
        try:
            with open(CACHE_PATH) as fh:
                cache = json.load(fh)
        except Exception:
            return None
        if cache.get("days") != self.days:
            return None
        if time.time() - cache.get("computed", 0) > 86400:
            return None
        return cache.get("buckets")

    def _store_cache(self, buckets):
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            tmp = CACHE_PATH + ".tmp"
            with open(tmp, "w") as fh:
                json.dump({"computed": int(time.time()), "days": self.days,
                           "buckets": buckets}, fh, indent=2)
            os.replace(tmp, CACHE_PATH)
        except Exception as exc:
            log("wait-stats cache write failed: %s" % exc)

    def _compute_waits(self, partitions):
        """One sacct pass over every partition we care about, bucketed by size."""
        samples = {}
        try:
            proc = subprocess.run(
                ["sacct", "-a", "-r", ",".join(sorted(partitions)), "-X",
                 "-S", "now-%ddays" % self.days, "-n", "-P",
                 "-o", "Partition,NNodes,Planned,State"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
            if proc.returncode != 0:
                log("sacct wait-stats failed: %s" % proc.stderr.decode()[:200])
                return {}
            for line in proc.stdout.decode().splitlines():
                bits = line.split("|")
                if len(bits) < 4 or bits[3].startswith("PENDING"):
                    continue
                try:
                    nodes = int(bits[1])
                except ValueError:
                    continue
                key = "%s/%s" % (bits[0], size_bucket(nodes))
                samples.setdefault(key, []).append(parse_slurm_duration(bits[2]) / 3600.0)
        except Exception as exc:
            log("wait-stats computation failed: %s" % exc)
            return {}
        buckets = {}
        for key, vals in samples.items():
            vals.sort()
            buckets[key] = {"n": len(vals),
                            "median": percentile(vals, 0.5),
                            "p90": percentile(vals, 0.9)}
        return buckets

    def typical_wait(self, job, all_partitions):
        if self._waits is None:
            cached = self._load_cache()
            if cached is None:
                cached = self._compute_waits(all_partitions)
                if cached:
                    self._store_cache(cached)
            self._waits = cached or {}
        partition = self._first_partition(job.get("partition"))
        bucket = size_bucket(job.get("nodes"))
        stat = self._waits.get("%s/%s" % (partition, bucket))
        if not stat or stat["n"] < self.min_sample:
            return None
        stat = dict(stat)
        stat["bucket"] = bucket
        return stat


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------
def job_lines(job, now, tz, prev_state, ctx=None, partitions=None):
    """Return (headline, detail lines) for one job, in Slack mrkdwn."""
    state = job["state"]
    icon = STATE_ICON.get(state, ":white_circle:")
    bits = [job["partition"] or "?"]
    if job["nodes"]:
        bits.append("%s node%s" % (job["nodes"], "" if job["nodes"] == 1 else "s"))
    if job["gpus"]:
        bits.append("%s GPU" % job["gpus"])
    if job["limit_min"]:
        bits.append("limit %s" % fmt_dur(job["limit_min"] * 60))
    headline = "%s `%s` *%s* — %s" % (icon, job["job_id"], job["name"], " · ".join(bits))

    lines = []
    status = state
    if job["reason"] and job["reason"] not in ("None", "none"):
        status += " (%s)" % job["reason"]
    if prev_state and prev_state != state:
        status += "  ← was %s" % prev_state
    lines.append("status: *%s*" % status)

    submitted = fmt_ts(job["submit"], tz)
    sub_line = "submitted: %s" % (submitted or "unknown")
    if job["submit"]:
        sub_line += " (%s)" % rel(job["submit"], now)
    lines.append(sub_line)

    # start
    if state == "PENDING":
        if job["start"]:
            lines.append("est. start: %s (%s)" % (fmt_ts(job["start"], tz), rel(job["start"], now)))
        else:
            waited = fmt_dur(now - job["submit"]) if job["submit"] else "?"
            lines.append("est. start: unknown — queued %s, scheduler gives no estimate" % waited)
        if ctx is not None:
            pos = ctx.position(job)
            if pos:
                lines.append("queue position: %s of %d by priority (%d ahead, %s node-h requested)"
                             % (ordinal(pos["rank"]), pos["total"], pos["ahead"],
                                format(int(round(pos["node_hours"])), ",")))
            wait = ctx.typical_wait(job, partitions or set())
            if wait:
                line = ("typical wait: %s jobs here — median %s, p90 %s (n=%d, %dd)"
                        % (wait["bucket"], fmt_hours(wait["median"]),
                           fmt_hours(wait["p90"]), wait["n"], ctx.days))
                queued_h = (now - job["submit"]) / 3600.0 if job["submit"] else 0
                if queued_h > wait["p90"] > 0:
                    line += " — *already past p90*"
                lines.append(line)
    elif job["start"]:
        started = "started: %s (%s)" % (fmt_ts(job["start"], tz), rel(job["start"], now))
        if state == "RUNNING":
            started += " · elapsed %s" % fmt_dur(now - job["start"])
        lines.append(started)
    else:
        lines.append("started: never")

    # finish
    if job["finished"]:
        elapsed = job.get("elapsed") or (job["end"] - job["start"] if job["start"] else 0)
        lines.append("finished: %s (%s) · ran %s"
                     % (fmt_ts(job["end"], tz), rel(job["end"], now), fmt_dur(elapsed)))
    elif state == "RUNNING" and job["end"]:
        lines.append("est. finish: %s (%s, at wall limit)"
                     % (fmt_ts(job["end"], tz), rel(job["end"], now)))
    elif state == "PENDING" and job["start"] and job["limit_min"]:
        est_end = job["start"] + job["limit_min"] * 60
        lines.append("est. finish: %s (%s, if it starts on estimate)"
                     % (fmt_ts(est_end, tz), rel(est_end, now)))
    elif job["limit_min"]:
        lines.append("est. finish: unknown — %s of wall time after it starts"
                     % fmt_dur(job["limit_min"] * 60))
    else:
        lines.append("est. finish: unknown")

    script = os.path.basename(job["command"]) if job["command"] else ""
    if script:
        lines.append("script: `%s`" % script)
    return headline, lines


def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return "%d%s" % (n, suffix)


def build_report(cfg, now, tz, active, finished, prev_states, since_epoch):
    header = "*Slurm report* · %s" % datetime.fromtimestamp(now, tz).strftime(
        "%a %d %b %Y %H:%M %Z")
    counts = {}
    for job in active:
        counts[job["state"]] = counts.get(job["state"], 0) + 1
    summary_bits = ["%d %s" % (v, k.lower()) for k, v in sorted(counts.items())]
    if finished:
        summary_bits.append("%d finished since %s"
                            % (len(finished), fmt_ts(since_epoch, tz)))
    if not summary_bits:
        summary_bits = ["no jobs in queue"]
    header += "  —  " + " · ".join(summary_bits)

    sections = []
    ctx = QueueContext(cfg) if cfg.get("queue_context", True) else None
    partitions = set()
    for job in active:
        if job["state"] == "PENDING" and job.get("partition"):
            partitions.add(job["partition"].split(",")[0].strip())

    def order_key(job):
        try:
            rank = STATE_ORDER.index(job["state"])
        except ValueError:
            rank = len(STATE_ORDER)
        return (rank, -(job["start"] or 0), job["submit"])

    for job in sorted(active, key=order_key):
        headline, lines = job_lines(job, now, tz, prev_states.get(str(job["job_id"])),
                                    ctx=ctx, partitions=partitions)
        sections.append(headline + "\n" + "\n".join("        " + ln for ln in lines))

    if finished:
        sections.append("*— finished since last report —*")
        for job in finished[: cfg["max_finished"]]:
            headline, lines = job_lines(job, now, tz, prev_states.get(str(job["job_id"])))
            sections.append(headline + "\n" + "\n".join("        " + ln for ln in lines))
        if len(finished) > cfg["max_finished"]:
            sections.append("_…and %d more finished jobs (see `sacct`)_"
                            % (len(finished) - cfg["max_finished"]))

    if not sections:
        sections.append("_Nothing queued or running, and nothing finished since the last report._")
    return header, sections


def to_plain_text(header, sections):
    def strip(s):
        s = s.replace("*", "").replace("`", "")
        # only unwrap whole-line _italics_, never underscores inside names
        return re.sub(r"^_(.*)_$", r"\1", s.strip()) if s.strip().startswith("_") else s
    out = [strip(header), ""]
    for sec in sections:
        for line in sec.split("\n"):
            line = strip(line)
            # drop slack emoji shortcodes
            while ":" in line:
                a = line.find(":")
                b = line.find(":", a + 1)
                if b == -1 or " " in line[a:b]:
                    break
                line = (line[:a] + line[b + 1:]).lstrip()
            out.append(line)
        out.append("")
    return "\n".join(out).strip() + "\n"


# --------------------------------------------------------------------------
# delivery
# --------------------------------------------------------------------------
def http_post(url, payload, headers=None):
    body = json.dumps(payload).encode()
    hdrs = {"Content-Type": "application/json"}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()[:400]
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()[:400]


def send_slack(cfg, header, sections):
    url = cfg.get("slack_webhook_url")
    if not url:
        raise SystemExit(
            "No Slack webhook configured. Put the URL in %s as \"slack_webhook_url\", "
            "or set $SLACK_WEBHOOK_URL." % CONFIG_PATH)
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": header[:2900]}},
              {"type": "divider"}]
    for sec in sections:
        if len(blocks) >= 48:
            blocks.append({"type": "context", "elements": [
                {"type": "mrkdwn", "text": "_report truncated — too many jobs for one message_"}]})
            break
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": sec[:2900]}})
    payload = {"text": to_plain_text(header, sections)[:3000], "blocks": blocks}
    status, body = http_post(url, payload)
    if status != 200:
        raise RuntimeError("Slack webhook returned %s: %s" % (status, body))
    return "slack ok"


def send_email(cfg, header, sections):
    conf = cfg["email"]
    key, to, sender = conf.get("api_key"), conf.get("to"), conf.get("from")
    if not (key and to and sender):
        raise SystemExit(
            "Email needs email.api_key, email.from and email.to in %s "
            "(SMTP is blocked on this cluster, so an HTTPS API is required)." % CONFIG_PATH)
    subject = header.replace("*", "")
    text = to_plain_text(header, sections)
    provider = (conf.get("provider") or "resend").lower()
    if provider == "resend":
        status, body = http_post(
            "https://api.resend.com/emails",
            {"from": sender, "to": [to], "subject": subject, "text": text},
            {"Authorization": "Bearer %s" % key})
        ok = status in (200, 201)
    elif provider == "sendgrid":
        status, body = http_post(
            "https://api.sendgrid.com/v3/mail/send",
            {"personalizations": [{"to": [{"email": to}]}],
             "from": {"email": sender},
             "subject": subject,
             "content": [{"type": "text/plain", "value": text}]},
            {"Authorization": "Bearer %s" % key})
        ok = status in (200, 202)
    else:
        raise SystemExit("Unknown email provider: %s" % provider)
    if not ok:
        raise RuntimeError("%s returned %s: %s" % (provider, status, body))
    return "email ok"


def deliver(cfg, channel, header, sections):
    if channel == "stdout":
        sys.stdout.write(to_plain_text(header, sections))
        return "stdout"
    if channel == "slack":
        return send_slack(cfg, header, sections)
    if channel == "email":
        return send_email(cfg, header, sections)
    raise SystemExit("Unknown channel: %s" % channel)


# --------------------------------------------------------------------------
# cron
# --------------------------------------------------------------------------
CRON_TAG = "# slurm-monitor (managed by slurm_monitor.py)"


def current_crontab():
    proc = subprocess.run(["crontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.stdout.decode() if proc.returncode == 0 else ""


def write_crontab(text):
    proc = subprocess.run(["crontab", "-"], input=text.encode(), stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError("crontab install failed: %s" % proc.stderr.decode())


def strip_managed(text):
    out, skip = [], False
    for line in text.splitlines():
        if line.strip() == CRON_TAG:
            skip = True
            continue
        if skip:
            if line.startswith("CRON_TZ=") or line.strip().startswith(("0 ", "#")) and "slurm_monitor" in line:
                continue
            if "slurm_monitor.py" in line:
                continue
            skip = False
        if "slurm_monitor.py" in line:
            continue
        out.append(line)
    return "\n".join(out).strip()


def install_cron(cfg, hours="8,20"):
    script = os.path.abspath(__file__)
    python = "/usr/bin/python3" if os.path.exists("/usr/bin/python3") else sys.executable
    tz = cfg.get("timezone") or "Asia/Taipei"
    entry = "\n".join([
        CRON_TAG,
        "CRON_TZ=%s" % tz,
        "0 %s * * * %s %s >> %s 2>&1" % (hours, python, script, LOG_PATH),
    ])
    body = strip_managed(current_crontab())
    new = (body + "\n\n" if body else "") + entry + "\n"
    write_crontab(new)
    return entry


def uninstall_cron():
    body = strip_managed(current_crontab())
    write_crontab(body + "\n" if body else "\n")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="print the report instead of sending")
    ap.add_argument("--channel", choices=["slack", "email", "stdout"], help="override config channel")
    ap.add_argument("--test", action="store_true", help="send a short test message and exit")
    ap.add_argument("--install-cron", action="store_true", help="schedule 08:00 and 20:00")
    ap.add_argument("--uninstall-cron", action="store_true", help="remove the schedule")
    ap.add_argument("--hours", default="8,20", help="cron hours for --install-cron (default 8,20)")
    ap.add_argument("--since", help="report jobs finished since this time (e.g. '2026-09-13T08:00')")
    ap.add_argument("--set-webhook", metavar="URL",
                    help="store a Slack incoming-webhook URL in the config file")
    args = ap.parse_args()

    cfg = load_config()
    tz = tzinfo(cfg)

    if args.set_webhook:
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        try:
            with open(CONFIG_PATH) as fh:
                raw = json.load(fh)
        except Exception:
            raw = {}
        raw["slack_webhook_url"] = args.set_webhook.strip()
        raw.setdefault("channel", "slack")
        with open(CONFIG_PATH, "w") as fh:
            json.dump(raw, fh, indent=2)
        os.chmod(CONFIG_PATH, 0o600)
        print("Saved webhook to %s (mode 600). Try: %s --test" % (CONFIG_PATH, __file__))
        return

    if args.uninstall_cron:
        uninstall_cron()
        print("Removed slurm-monitor cron entries.")
        return
    if args.install_cron:
        print("Installed:\n%s" % install_cron(cfg, args.hours))
        return

    channel = args.channel or ("stdout" if args.dry_run else cfg["channel"])

    if args.test:
        header = "*Slurm monitor test* · %s" % datetime.now(tz).strftime("%a %d %b %H:%M %Z")
        sections = ["If you can read this, reports will arrive at 08:00 and 20:00 %s."
                    % (cfg.get("timezone") or "local time")]
        print(deliver(cfg, channel, header, sections))
        return

    if not cfg["user"]:
        raise SystemExit("Cannot determine user; set \"user\" in %s" % CONFIG_PATH)

    now = int(time.time())
    state = load_state()
    if args.since:
        since = int(datetime.strptime(args.since, "%Y-%m-%dT%H:%M").replace(tzinfo=tz).timestamp())
    else:
        # everything that finished since the previous *delivered* report;
        # dry runs never advance the cursor, so nothing gets skipped
        since = state.get("last_run") or (now - cfg["finished_lookback_hours"] * 3600)

    active = fetch_active(cfg["user"])
    # a broken sacct must not cost you the whole report -- squeue alone still
    # tells you what is queued and running
    degraded = ""
    try:
        finished = fetch_finished(cfg["user"], since, tz)
    except Exception as exc:
        log("sacct lookup failed, reporting active jobs only: %s" % exc)
        finished = []
        degraded = "sacct unavailable — finished jobs omitted from this report"
    active_ids = set(str(j["job_id"]) for j in active)
    finished = [j for j in finished if str(j["job_id"]) not in active_ids]

    prev_states = state.get("job_states", {})
    header, sections = build_report(cfg, now, tz, active, finished, prev_states, since)
    if degraded:
        sections.append("_%s_" % degraded)

    result = deliver(cfg, channel, header, sections)
    if not args.dry_run and not args.since:
        save_state({
            "last_run": now,
            "job_states": dict((str(j["job_id"]), j["state"]) for j in active),
        })
    log("sent via %s: %d active, %d finished (%s)"
        % (channel, len(active), len(finished), result))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # keep cron output short but useful
        log("ERROR: %s" % exc)
        sys.stderr.write("slurm-monitor error: %s\n" % exc)
        sys.exit(1)
