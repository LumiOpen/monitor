import logging
from datetime import datetime, timedelta

from slurmmonitor.lumi.allocations import get_lumi_allocations

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
            f"GPU quota {project}: used {_human_int(used)}/{_human_int(allocated)} GPUh "
            f"({_pct(used_pct)}), {days_remaining} days remain"
        )

        if updated_at:
            age = now_real - updated_at
            if age > timedelta(hours=24):
                hours = int(age.total_seconds() // 3600)
                msg += f" [WARNING: lumi-allocations data stale ({hours}h old)]"

        lines.append(msg)

    return lines
