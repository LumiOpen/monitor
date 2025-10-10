import logging
import re
from datetime import datetime

from slurmmonitor.slurm.util import run_or_raise

logger = logging.getLogger(__name__)


def _parse_int(token: str):
    if token is None:
        return None
    token = token.replace(",", "").strip()
    try:
        return int(token)
    except Exception:
        return None


def _parse_updated_at(line: str):
    m = re.search(r"Data updated:\s*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})", line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def parse_lumi_allocations(text: str):
    """Parse the output of `lumi-allocations`.

    Returns a dict: { 'updated_at': datetime|None, 'projects': { name: { 'gpu_used': int, 'gpu_allocated': int } } }
    """
    updated_at = None
    projects = {}

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith("Data updated:"):
            updated_at = _parse_updated_at(line)
            continue
        if not line.startswith("project_"):
            continue

        # Expected format with '|' separators, GPU column is the third (index 2)
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        project = parts[0]
        gpu_col = parts[2]

        m = re.search(r"([0-9][0-9,]*)/([0-9][0-9,]*)", gpu_col)
        if not m:
            # No numeric used/allocated found for GPU column
            continue
        used = _parse_int(m.group(1)) or 0
        allocated = _parse_int(m.group(2)) or 0

        projects[project] = {
            "gpu_used": used,
            "gpu_allocated": allocated,
        }

    return {"updated_at": updated_at, "projects": projects}


def get_lumi_allocations():
    """Run `lumi-allocations` and parse its output."""
    output = run_or_raise("lumi-allocations")
    return parse_lumi_allocations(output)

