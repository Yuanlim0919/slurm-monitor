# slurm-monitor

Twice-daily Slack (or email) digests of your Slurm jobs, so you stop running `squeue`
every twenty minutes.

One message at 08:00 and one at 20:00 covering every job you have queued or running,
plus everything that finished since the previous report. For each job: **job id,
submit time, a one-line summary, current status, estimated-or-actual start, and
estimated-or-actual finish.**

```
Slurm report · Sun 13 Sep 2026 22:18 CST  —  4 pending · 2 finished since Wed 09 Sep 00:00

358841 stormer_patch4_convproj — gb200-r1 · 4 nodes · 16 GPU · limit 1d 0h
        status: PENDING (Priority)
        submitted: Tue 08 Sep 20:29 (5d 1h ago)
        est. start: unknown — queued 5d 1h, scheduler gives no estimate
        est. finish: unknown — 1d 0h of wall time after it starts
        script: train_stormer_patch4_convproj_gb200r1.sh

371350 narrow-v-wind — 8gpus · 1 node · 1 GPU · limit 1h 0m
        status: COMPLETED
        submitted: Thu 10 Sep 12:50 (3d 9h ago)
        started: Thu 10 Sep 12:50 (3d 9h ago)
        finished: Thu 10 Sep 12:53 (3d 9h ago) · ran 3m
        script: narrow_v_wind.sbatch
```

In Slack each job renders as its own block with a state emoji. A job whose state changed
since the last report is tagged `← was PENDING`.

## Requirements

- Slurm **23.11 or newer** on a login node — the tool reads `squeue --json` and `sacct --json`
- Python **3.9+**, standard library only. No `pip install`, no virtualenv.
- Outbound HTTPS from the login node (for the Slack webhook)
- `crond` running on the login node, and permission to use `crontab`

## Install

```bash
git clone <this-repo> ~/slurm-monitor
cd ~/slurm-monitor

mkdir -p ~/.config/slurm-monitor
cp config.example.json ~/.config/slurm-monitor/config.json
chmod 600 ~/.config/slurm-monitor/config.json
```

See what a report looks like before wiring up any delivery:

```bash
./slurm_monitor.py --dry-run
```

### Set up the Slack webhook

1. Go to <https://api.slack.com/apps> → **Create New App** → **From scratch**, name it
   anything, pick your workspace.
2. **Incoming Webhooks** → toggle **On** → **Add New Webhook to Workspace**.
3. Choose where reports land. A private channel or a DM to yourself is fine.
4. Copy the `https://hooks.slack.com/services/...` URL and store it:

```bash
./slurm_monitor.py --set-webhook 'https://hooks.slack.com/services/...'
./slurm_monitor.py --test     # should appear in Slack within a second
./slurm_monitor.py            # send a real report right now
```

The webhook is a bearer credential — anyone holding it can post to that channel. It is
written to `~/.config/slurm-monitor/config.json` with mode `600`, and `.gitignore` keeps
`config.json` out of git. If you would rather not store it on disk at all, export
`SLACK_WEBHOOK_URL` instead; the environment always wins over the config file.

### Schedule it

```bash
./slurm_monitor.py --install-cron          # 08:00 and 20:00
./slurm_monitor.py --install-cron --hours 7,13,19   # or pick your own
crontab -l
```

The entry carries `CRON_TZ`, so the times mean what your configured `timezone` says they
mean regardless of the node's clock. Re-running `--install-cron` replaces the old entry
rather than stacking a second one. `--uninstall-cron` removes it.

**Cron is per-login-node.** The crontab lives on whichever login node you ran the command
on. If your cluster has several and that one goes down, the reports stop — re-run
`--install-cron` elsewhere (`$HOME` is usually shared, so nothing else needs moving).
The reports themselves cover all your jobs cluster-wide either way.

## Email instead of Slack

Many clusters block outbound SMTP from login nodes, which rules out sending mail
directly. The email backend therefore talks to a transactional-mail HTTPS API. Check
whether SMTP is even an option on your cluster:

```bash
timeout 5 bash -c 'cat </dev/null >/dev/tcp/smtp.gmail.com/587' && echo open || echo blocked
```

If it is blocked, sign up for [Resend](https://resend.com) or
[SendGrid](https://sendgrid.com) (both have free tiers), verify a sender address, then:

```json
{
  "channel": "email",
  "email": {
    "provider": "resend",
    "api_key": "re_...",
    "from": "slurm-bot@your-verified-domain",
    "to": "you@example.com"
  }
}
```

Worth knowing: Slurm's own `--mail-type=BEGIN,END,FAIL` is sent by the *controller*, not
by your login node, so it is unaffected by that block. This tool complements it — Slurm
mails you per event, this mails you a periodic view of everything at once.

## Configuration

`~/.config/slurm-monitor/config.json`:

| Key | Default | Meaning |
|---|---|---|
| `channel` | `"slack"` | `slack`, `email`, or `stdout` |
| `slack_webhook_url` | `""` | Incoming-webhook URL; `$SLACK_WEBHOOK_URL` overrides |
| `email.provider` | `"resend"` | `resend` or `sendgrid` |
| `email.api_key` | `""` | API key; `$SLURM_MONITOR_EMAIL_KEY` overrides |
| `email.from` / `email.to` | `""` | Verified sender / your address |
| `user` | `$USER` | Slurm account to report on |
| `timezone` | `"Asia/Taipei"` | IANA name; used for timestamps and `CRON_TZ` |
| `finished_lookback_hours` | `14` | How far back the *first* report looks for finished jobs |
| `max_finished` | `15` | Cap on finished jobs listed per report |

Point `$SLURM_MONITOR_CONFIG` elsewhere to use a different config file.

## Command line

| Flag | Effect |
|---|---|
| *(none)* | Build the report and send it on the configured channel |
| `--dry-run` | Print to stdout, send nothing, don't advance the state cursor |
| `--channel {slack,email,stdout}` | Override the configured channel for one run |
| `--test` | Send a short "it works" message |
| `--set-webhook URL` | Store a Slack webhook in the config file (mode 600) |
| `--install-cron` / `--uninstall-cron` | Manage the schedule |
| `--hours 8,20` | Cron hours for `--install-cron` |
| `--since 2026-09-13T08:00` | Report jobs finished since a specific time (doesn't advance the cursor) |

## How it works

Active jobs come from `squeue -u $USER --json`; jobs that ended come from
`sacct -u $USER -X --json -S <since>`. `<since>` is the timestamp of the last
*delivered* report, kept in `~/.local/state/slurm-monitor/state.json`, so nothing gets
reported twice and nothing falls through the gap. Dry runs deliberately don't move that
cursor.

Start and finish times are reported as follows:

| Job state | Start | Finish |
|---|---|---|
| `PENDING`, scheduler has a backfill estimate | est. start | est. start + wall limit |
| `PENDING`, no estimate | `unknown`, with how long it has been queued | `unknown`, with the wall limit |
| `RUNNING` | actual start + elapsed | wall-clock deadline |
| terminal | actual start | actual end + run duration |

The tool never invents a number. If the scheduler returns `StartTime=Unknown` — common on
busy clusters with no backfill window for large jobs — the report says so plainly rather
than guessing.

## Troubleshooting

Every run appends to `~/.local/state/slurm-monitor/monitor.log`, and cron failures land
there too:

```bash
tail ~/.local/state/slurm-monitor/monitor.log
```

**No report arrived.** Check `crond` is up (`systemctl is-active crond`), that `crontab -l`
still shows the entry, and that you're on the login node where you installed it.

**"Slack webhook returned 403/404".** The webhook was revoked or the app was removed from
the workspace. Make a new one and re-run `--set-webhook`.

**`squeue: unrecognized option '--json'`.** Slurm is older than 23.11. Nothing to be done
short of parsing `squeue -O` output instead.

**Cloning on a cluster that blocks SSH port 22.** GitHub also serves SSH on 443:

```bash
git clone ssh://git@ssh.github.com:443/<owner>/slurm-monitor.git
```

## License

MIT — see [LICENSE](LICENSE).
