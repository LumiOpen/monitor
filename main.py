import collections
import datetime
import json
import os
import requests
import re
import slurm.util
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

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
        return time.time() - mtime > 10800 # 3 hours to allow for very slow starts

    def progress(self):
        if self.latest is None or self.total is None:
            print("not configured")
            return " "
        try:
            with open(self.latest, "r") as f: 
                line = f.readlines()[0]
        except Exception as e:
            print(f"exception: {e}")
            return " "

        groups = re.search("(\d+)$", line)
        if not groups:
            print("no match")
            return " "
        
        step = int(groups[0])
        progress = step / self.total * 100
        return f" {progress:2.1f}% "

    def __str__(self):
        return self.name

job_config = [
    Job("viking_v2_7B_high_eps",
        logfile='/scratch/project_462000319/production/logs-7B_high_eps/latest.out',
        latest='/scratch/project_462000086/viking-v2/7B_high_eps/latest_checkpointed_iteration.txt',
        total=476837,
    ),
    Job("viking_v2_7B_nofa",
        logfile='/scratch/project_462000319/production/logs-7B_nofa/latest.out',
        latest='/scratch/project_462000086/viking-v2/7B_nofa/latest_checkpointed_iteration.txt',
        total=476837,
    ),
    Job("warmstart_7B_viking_v3_64",
        logfile="/scratch/project_462000319/rluukkon/megatron_tests/Megatron-LM-test2/logs-7B_high_eps/latest.out",
        latest="/scratch/project_462000086/risto/viking-v3/v2_converted_checkpoints/viking_v2_7B_iter_0358000_bfloat16/latest_checkpointed_iteration.txt",
        total=118837,
    ),
    Job("viking_v2_13B_high_eps",
        logfile='/scratch/project_462000319/production/logs-13B_high_eps/latest.out',
        latest='/scratch/project_462000086/viking-v2/13B_high_eps/latest_checkpointed_iteration.txt',
        total=476837,
    ),
    Job("viking_v2_33B_high_eps",
        logfile='/scratch/project_462000319/production/logs-33B_high_eps/latest.out',
        latest='/scratch/project_462000086/viking-v2/33B_high_eps/latest_checkpointed_iteration.txt',
        total=476837,
    ),

    Job("v3-train-70B",
        logfile='/flash/project_462000319/europa-production/logs/latest.out',
        latest='/scratch/project_462000353/europa-checkpoints/512N/latest_checkpointed_iteration.txt',
        total=572204,
    ),
    Job("v3-train-1024N-70B",
        logfile='/flash/project_462000319/europa-production/logs/latest_1024N.out',
        latest='/scratch/project_462000353/europa-checkpoints/1024N/latest_checkpointed_iteration.txt',
        total=572204,
    ),
]
users = [
    'jburdge',
    'pyysalos',
    'rluukkon',
    'avirtanen',
]
min_storage = {
    "/flash/project_462000319": 10e12,
    "/scratch/project_462000319": 10e12,
    "/flash/project_462000086": 1e12,
    "/scratch/project_462000086": 10e12,
    "/flash/project_462000444": 1e12,
    "/scratch/project_462000444": 10e12,
}
min_inodes = {
    "/flash/project_462000319": 1e5,
    "/scratch/project_462000319": 1e5,
    "/scratch/project_462000086": 1e5,
    "/flash/project_462000086": 1e5,
    "/scratch/project_462000444": 1e5,
    "/flash/project_462000444": 1e5,
}



def get_current_snapshot(job_names, users):
    jobs = {}
    jobs_running_count = collections.defaultdict(int)
    for job in slurm.util.get_job_state(users):
        # check for only the job names we're interested in.
        if job.name not in job_names:
            continue

        # names are not guaranteed unique, so let's look for duplicates here that might throw our monitoring off.
        if job.running:
            jobs_running_count[job.name] += 1

        seen_job = jobs.get(job.name, None)
        if seen_job is None:
            jobs[job.name] = job
        elif job.running:
            # running jobs should take precedence in status reporting over
            # pending jobs.
            jobs[job.name] = job

    return jobs, jobs_running_count

def get_free_space(path):
    stats = os.statvfs(path)
    #total_inodes = stats.f_files
    #free_inodes = stats.f_ffree
    block_size = stats.f_frsize
    free_blocks = stats.f_bfree
    return free_blocks * block_size

def get_free_inodes(path):
    stats = os.statvfs(path)
    free_inodes = stats.f_ffree
    return free_inodes


def post_msg(message):
    print(message)
    headers = {'Content-Type': 'application/json'}
    payload = {'text': message}

    webhook_url = os.getenv("WEBHOOK_URL")
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        print(f"Error posting to slack: {response.status_code}, {response.text}")

def format_seconds(seconds):
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    formatted = "%02dd%02dh%02dm%02ds" % (days, hours, minutes, seconds)
    return formatted


def get_progress(job_state):
    for config in job_config:
        if job_state.name == config.name:
            progress = config.progress()
            return progress
    return ""

def main():
    last_state = {}
    last_time = datetime.datetime.now()
    storage_warnings_active = {}
    inode_warnings_active = {}
    stall_warnings_active = {}
    first_run = True
    while True:
        try:
            jobs, jobs_running = get_current_snapshot([job.name for job in job_config], users)
        except subprocess.CalledProcessError as e:
            print(f"error getting snapshot from slurm: {e.output}")
            time.sleep(10)
            continue

        for job_name, count in jobs_running.items():
            if count > 1:
                print(f"WARNING: {job_name} running {count} times!")

        # morning message
        messages = []
        current_time = datetime.datetime.now()
        if last_time.hour == 8 and current_time.hour == 9:
            messages.append("Morning job status:")
            for job in sorted(jobs.values(), key=lambda x: x.name):
                time_left = format_seconds(job.time_left)
                progress = get_progress(job)
                messages.append(f"{job.name}{progress}{job.job_id} {job.state} {job.emoji} {time_left} remaining")

            for job in job_config:
                current_job = jobs.get(job.name, None)
                if current_job is not None:
                    if current_job.running and job.stalled():
                        messages.append(f"{job.name} looks stalled: {job.logfile}")


        last_time = current_time
        if messages:
            post_msg("\n".join(messages))

        # stateful messages
        # job status
        messages = []
        for job in job_config:
            last_job = last_state.get(job.name, None)
            current_job = jobs.get(job.name, None)


            if last_job is None:
                if current_job is not None:
                    time_left = format_seconds(current_job.time_left)
                    progress = get_progress(job)
                    messages.append(f"New job detected: {job}{progress}{current_job.job_id} in {current_job.state} {current_job.emoji} ({time_left} remaining)")
            elif current_job is None:
                time_left = format_seconds(last_job.time_left)
                progress = get_progress(job)
                messages.append(f"Job ended: {job}{progress}{last_job.job_id} last known state {last_job.state}")
            elif last_job.running != current_job.running:
                time_left = format_seconds(current_job.time_left)
                progress = get_progress(job)
                messages.append(f"Job changed state: {job.name}{progress}{last_job.job_id}:{last_job.state} {last_job.emoji} -> {current_job.job_id}:{current_job.state} {current_job.emoji} ({time_left} remaining")
            elif current_job.running:
                # check if job is stalled
                active_warning = stall_warnings_active.get(job.name, False)
                stalled = job.stalled()

                if active_warning:
                    if not stalled:
                        stall_warnings_active[job.name] = False
                        messages.append(f"{job.name} no longer appears to be stalled.")
                elif stalled:
                    stall_warnings_active[job.name] = True
                    messages.append(f"{job.name} looks stalled: {job.logfile}")

            last_state[job.name] = current_job

                
                

        # free space
        for path, min_free in min_storage.items():
            active_warning = storage_warnings_active.get(path, False)
            current_free = get_free_space(path)

            if active_warning:
                if current_free > min_free:
                    messages.append(f"{path} has sufficient free space ({current_free} > {min_free})")
                    storage_warnings_active[path] = False
            elif current_free < min_free:
                storage_warnings_active[path] = True
                messages.append(f"Warning: {path} has insufficient free space ({current_free} < {min_free})")


        # free inodes
        for path, min_free in min_inodes.items():
            active_warning = inode_warnings_active.get(path, False)
            current_free = get_free_inodes(path)

            if active_warning:
                if current_free > min_free:
                    messages.append(f"{path} has sufficient free inodes ({current_free} > {min_free})")
            elif current_free < min_free:
                inode_warnings_active[path] = True
                messages.append(f"Warning: {path} has insufficient free inodes ({current_free} < {min_free})")
        

        # don't post any stateful messages on startup.
        if first_run:
            print("\n".join(messages))
            first_run = False
            continue
        if messages:
            post_msg("\n".join(messages))

        with open("log.jsonl", "a") as f:
            f.write(json.dumps({
                "timestamp": time.time(),
                "job_state": {k: v.model_dump() for k,v in last_state.items() if v is not None},
            }) + "\n")

        time.sleep(60)


if __name__ == "__main__":
    main()
