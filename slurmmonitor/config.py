import os
import re
import time


class Job:
    def __init__(self, name, logfile=None, latest=None, total=None):
        self.name = name
        self.logfile = logfile
        self.latest = latest
        self.total = total

    def stalled(self):
        if self.logfile is None:
            return False

        if not os.path.exists(self.logfile):
            return True
        
        mtime = os.path.getmtime(self.logfile)
        return time.time() - mtime > 10800  # 3 hours to allow for very slow starts

    def progress(self):
        if self.latest is None or self.total is None:
            return "na"
        try:
            with open(self.latest, "r") as f: 
                line = f.readlines()[0]
        except Exception as e:
            return "?"

        groups = re.search("(\d+)$", line)
        if not groups:
            print("no match")
            return "?"
        
        step = int(groups[0])
        progress = step / self.total * 100
        return f"{progress:2.1f}%"

    def __str__(self):
        return self.name

job_config = [
    #Job("7B_europa_64",
    #    logfile='/scratch/project_462000353/europa-production/logs-7B/latest.out',
    #    latest='/scratch/project_462000353/europa-checkpoints/7B_checkpoints/latest_checkpointed_iteration.txt',
    #    total=715256,
    #),
        
]

users = [
    'jburdge',
    'pyysalos',
    'rluukkon',
    'avirtanen',
]

free_bytes_config = {
    "/flash/project_462000963": 1e12,
    "/scratch/project_462000963": 10e12,
}

free_inodes_config = {
    "/scratch/project_462000963": 1e5,
    "/flash/project_462000963": 1e5,
}

slurm_partitions = [
    "standard-g",
    "small-g"
]

# Projects to track for GPU quota usage. Keys must match the project names
# reported by `lumi-allocations` (e.g., 'project_462000963'). Dates are ISO
# formatted (YYYY-MM-DD). Optional milestone tracks an intermediate spend goal
# until its date passes.
gpu_quota_projects = {
    "project_462000963": {
        "start": "2025-05-22",
        "end": "2026-05-31",
        # "milestone": {
        #     "name": "retention checkpoint",
        #     "date": "2026-02-01",
        #     "target_pct": 50.0,
        # },
    },
    "project_465002530": {
        # End date is intentionally omitted until confirmed; this avoids
        # reporting a misleading overall burn-down trajectory.
        "start": "2026-05-29",
        "milestone": {
            "name": "resource cut checkpoint",
            "kind": "protect",
            "date": "2026-08-02",
            "target_pct": 40.0,
        },
    },
    "project_462001516": {
        # Assumed allocation period; confirm once lumi-allocations exposes it.
        "start": "2026-05-29",
        "end": "2027-05-31",
        "milestone": {
            "name": "second-half allocation checkpoint",
            "kind": "unlock",
            # The review is described as "late October"; use Oct 31 until a
            # more specific date is known.
            "date": "2026-10-31",
            "target_mode": "linear",
            "target_base_gpuh": 2_000_000,
        },
    },
}
