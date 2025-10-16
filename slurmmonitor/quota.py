import logging
from datetime import datetime, timedelta
import re

from slurmmonitor.lumi.allocations import get_lumi_allocations
from slurmmonitor.slurm.util import run_or_raise

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def _clamp(dt: datetime, start: datetime, end: datetime) -> datetime:
    if dt < start:
        return start
    if dt > end:
        return end
    return dt


def _pct(x: float) -> str:
    return f"{x:.1f}%"


def _human_int(n: int) -> str:
    return f"{n:,}"

def _human_k(n: int) -> str:
    """Format an integer as thousands with one decimal and 'K' suffix.

    Example: 172100 -> '172.1K'. No comma separators.
    """
    return f"{(n/1000.0):.1f}K"
def _elapsed_to_hours(time_str: str) -> float:
    """Convert Slurm elapsed time (D-HH:MM:SS or HH:MM:SS) to hours."""
    if not time_str:
        return 0.0
    days = 0
    parts = time_str.split("-")
    if len(parts) == 2:
        days = int(parts[0])
        clock = parts[1]
    else:
        clock = parts[0]
    h, m, s = [int(x) for x in clock.split(":")]
    return days * 24.0 + h + m / 60.0 + s / 3600.0

def _gpu_count_from_tres(alloc_tres: str) -> int:
    """Extract total GPUs from an AllocTRES field.

    Matches both 'gres/gpu=8' and 'gres/gpu:mi250x=8'.
    """
    m = re.search(r"gres/gpu[^=]*=(\d+)", alloc_tres or "")
    if not m:
        return 0
    return int(m.group(1))

def get_weekly_gpu_hours_by_project(projects: list[str]) -> dict[str, int]:
    """Return GPU-hours used in the last 7 days for each project (account).

    Uses sacct to gather elapsed time and allocated GPUs, then computes
    GPU-hours as ElapsedHours * GPUCount / 2 (LUMI MI250X has 2 GCDs).
    """
    if not projects:
        return {}

    now = datetime.now()
    start_dt = now - timedelta(days=7)
    start_s = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_s = now.strftime("%Y-%m-%dT%H:%M:%S")

    cmd = (
        f"sacct -a -A {','.join(projects)} --starttime {start_s} --endtime {end_s} "
        "--format Account,User,Elapsed,AllocTRES,Start -P"
    )
    out = run_or_raise(cmd)

    totals: dict[str, float] = {}
    for line in out.splitlines():
        if not line or line.startswith("Account|"):
            continue
        parts = line.split("|")
        if len(parts) < 4:
            continue
        account = parts[0].strip()
        user = parts[1].strip()
        elapsed = parts[2].strip()
        alloc_tres = parts[3].strip()
        if not account or not user:
            continue

        gpus = _gpu_count_from_tres(alloc_tres)
        if gpus <= 0:
            continue
        hours = _elapsed_to_hours(elapsed)
        # LUMI MI250X: sacct reports GPUs counting both GCDs, divide by 2
        gpu_hours = hours * (gpus / 2.0)
        totals[account] = totals.get(account, 0.0) + gpu_hours

    # Round to nearest integer GPUh for reporting
    return {k: int(round(v)) for k, v in totals.items()}


def compute_gpu_quota_messages(projects_cfg: dict):
    """Compute daily GPU quota status lines for the provided projects.

    - Uses `lumi-allocations` data as the source of truth.
    - Treats the data's "Data updated" timestamp as "now" for trend calculations.
    - Warns if the data looks stale (>24h old).
    """
    try:
        data = get_lumi_allocations()
    except Exception as e:
        logger.error(f"Error running lumi-allocations: {e}", exc_info=True)
        return [f"GPU quota: unable to fetch allocations ({e})"]

    updated_at = data.get("updated_at")
    projects = data.get("projects", {})
    now_real = datetime.now()

    # Try to compute weekly GPU-hours in one shot for all configured projects.
    weekly_by_project: dict[str, int] = {}
    try:
        weekly_by_project = get_weekly_gpu_hours_by_project(list(projects_cfg.keys()))
    except Exception as e:
        logger.warning(f"Unable to compute weekly GPU-hours via sacct: {e}")

    lines = []
    for project, cfg in projects_cfg.items():
        try:
            start_date = _parse_date(cfg["start"])  # YYYY-MM-DD (date-only)
            end_date = _parse_date(cfg["end"])      # YYYY-MM-DD (date-only)
        except Exception as e:
            lines.append(f"GPU quota {project}: invalid start/end dates in config ({e})")
            continue

        alloc_info = projects.get(project)
        if not alloc_info:
            lines.append(f"GPU quota {project}: no GPU allocation data")
            continue

        used = int(alloc_info.get("gpu_used") or 0)
        allocated = int(alloc_info.get("gpu_allocated") or 0)
        # Anchor start/end to the same time-of-day as the dataset timestamp (or now if missing)
        baseline = updated_at or now_real
        start = datetime.combine(start_date.date(), baseline.time())
        end = datetime.combine(end_date.date(), baseline.time())

        # If the allocation period has ended, report and skip trend math.
        if baseline >= end:
            msg = f"GPU quota {project}: allocation period ended on {end_date.strftime('%Y-%m-%d')}"
            if updated_at:
                age = now_real - updated_at
                if age > timedelta(hours=24):
                    hours = int(age.total_seconds() // 3600)
                    msg += f" [WARNING: lumi-allocations data stale ({hours}h old)]"
            lines.append(msg)
            continue

        if allocated <= 0:
            msg = f"GPU quota {project}: no allocated GPU hours (0)"
            if updated_at:
                age = now_real - updated_at
                if age > timedelta(hours=24):
                    hours = int(age.total_seconds() // 3600)
                    msg += f" [WARNING: lumi-allocations data stale ({hours}h old)]"
            lines.append(msg)
            continue

        used_pct = used / allocated * 100.0

        # Use the lumi-allocations update timestamp as the effective "now" for trend.
        now_for_trend = baseline
        now_for_trend = _clamp(now_for_trend, start, end)
        # Days remaining until end (clamped to [0, total_days])
        days_remaining = int((end - now_for_trend).total_seconds() // 86400)

        msg = (
            f"GPU quota {project}: used {_human_k(used)}/{_human_k(allocated)} GPUh "
            f"({_pct(used_pct)}), {days_remaining} days remain"
        )

        weekly_val = weekly_by_project.get(project)
        if weekly_val is not None:
            weekly_pct = weekly_val / allocated * 100.0
            msg += f", last 7d {_human_k(weekly_val)} GPUh ({_pct(weekly_pct)})"

            # Show target 7d consumption to finish on time and ETA at current pace
            remaining = max(allocated - used, 0)
            if days_remaining > 0 and remaining > 0:
                target_week = (remaining / days_remaining) * 7.0
                msg += f", target 7d {_human_k(target_week)} GPUh"

            if weekly_val > 0 and remaining > 0:
                daily_rate = weekly_val / 7.0
                eta_days = int(round(remaining / daily_rate)) if daily_rate > 0 else None
                if eta_days is not None and days_remaining > 0:
                    msg += f", ETA ~{eta_days}d/{days_remaining}d"

        if updated_at:
            age = now_real - updated_at
            if age > timedelta(hours=24):
                hours = int(age.total_seconds() // 3600)
                msg += f" [WARNING: lumi-allocations data stale ({hours}h old)]"

        lines.append(msg)

    return lines
