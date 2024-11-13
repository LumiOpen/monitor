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
    Job("7B_europa_64",
        logfile='/scratch/project_462000353/europa-production/logs-7B/latest.out',
        latest='/scratch/project_462000353/europa-checkpoints/7B_checkpoints/latest_checkpointed_iteration.txt',
        total=572204,
    ),
        
]

users = [
    'jburdge',
    'pyysalos',
    'rluukkon',
    'avirtanen',
]

free_bytes_config = {
    "/flash/project_462000353": 1e12,
    "/scratch/project_462000353": 10e12,
    "/flash/project_462000444": 1e12,
    "/scratch/project_462000444": 10e12,
}

free_inodes_config = {
    "/scratch/project_462000353": 1e5,
    "/flash/project_462000353": 1e5,
    "/scratch/project_462000444": 1e5,
    "/flash/project_462000444": 1e5,
}

slurm_partitions = [
    "standard-g",
    "small-g"
]
