import re
import subprocess
from datetime import datetime
from pydantic import BaseModel, validator



STATUS_RUNNING = [
    "RUNNING",
    "COMPLETING",
]

# these jobs are pending or potentially resumable
STATUS_PENDING = [
    "CONFIGURING",
    "PENDING",
    "RESV_DEL_HOLD",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "REQUEUED",
    "RESIZING",
    "SIGNALING",
    "STAGE_OUT",
    "STOPPED",       # unclear if this should be treated as a pending state
    "SUSPENDED",     # unclear if this should be treated as a pending state
]

# in all of these states, the job is not running and will not run again
# so it would be safe to restart if necessary.
STATUS_NOT_RUNNING = [
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
]

def parse_time(time_str):
    """Parses the time left string from squeue and converts it to seconds"""
    days = 0
    parts = time_str.split('-')
    if len(parts) == 2:
        days = int(parts[0])
        time_str = parts[1]

    try:
        segments = [int(i) for i in time_str.split(':')]
    except:
        print(f"Couldn't parse time: {time_str}")
        return 0

    if len(segments) == 3:
        hours, minutes, seconds = segments
    elif len(segments) == 2:
        hours = 0
        minutes, seconds = segments
    else:
        raise ValueError(f"Couldn't parse '{time_str}'")
    total_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
    return total_seconds

class JobState(BaseModel):
    job_id: int
    state: str
    name: str
    time_running: int
    time_left: int
    time_since_submit: int
    running: bool = False
    pending: bool = False
    emoji: str = "🤔"

    @validator('emoji', always=True)
    def check_emoji(cls, v, values):
        if values.get('state') in STATUS_RUNNING:
            return "✅"
        return "⏸️"

    @validator('running', always=True)
    def check_running(cls, v, values):
        return values.get('state') in STATUS_RUNNING

    @validator('pending', always=True)
    def check_pending(cls, v, values):
        return values.get('state') in STATUS_PENDING


# squeue -o '%i %T %j %M %L %V'
#JOBID STATE NAME TIME TIME_LEFT SUBMIT_TIME
# 4970726 RUNNING mmlu 12:09:46 1-11:50:14 2023-11-20T19:21:14
# 4971251 PENDING vik13B-3 0:00 2-00:00:00 2023-11-20T21:22:06
# 4967015 RUNNING data_pt33b128 19:52:41 1-04:07:19 2023-11-20T11:38:20
# 4958565 RUNNING pretrain_33B_128_node.sh 1-00:54:19 23:05:41 2023-11-20T06:34:00
def get_job_state(users):
    command = 'squeue -o "%i %T %j %M %L %V" -u ' + ",".join(users)
    status, output = subprocess.getstatusoutput(command)
    if status != 0:
        raise subprocess.CalledProcessError(status, command, output)
    
    return parse_job_state(output)

def parse_job_state(squeue_output):
    job_states = []
    output_lines = squeue_output.split('\n')
    for line in output_lines:
        if not line or "JOBID" in line:
            continue

        try:
            job_id, state, name, time_running, time_left, submit_time = line.split()
        except:
            raise ValueError(f"unable to parse line: {line}")

        time_running_seconds = parse_time(time_running)
        time_left_seconds = parse_time(time_left)
        # parse submit_time and calculate number of seconds since that time
        submit_time_datetime = datetime.strptime(submit_time, "%Y-%m-%dT%H:%M:%S")
        time_since_submit = int((datetime.now() - submit_time_datetime).total_seconds())
        job_state = JobState(
            job_id=int(job_id), 
            state=state, 
            name=name, 
            time_running=time_running_seconds, 
            time_left=time_left_seconds, 
            time_since_submit=time_since_submit
        )
        job_states.append(job_state)

    # return running jobs first, then sort by job id.
    # (we use not x.running here to invert the sort order for running jobs)
    return sorted(job_states, key=lambda x: (not x.running, x.job_id))


# calculates cluster days in running or pending state, ignoring jobs that are
# scheduled with a dependency
def get_queue_days(queue="standard-g"):
    command = f"scontrol show partition {queue}"
    status, output = subprocess.getstatusoutput(command)
    if status != 0:
        raise subprocess.CalledProcessError(status, command, output)

    m = re.search(r"TotalNodes=(\d+)", output)
    if m:
        node_count = int(m.group(1))
    else:
        raise Exception("Couldn't parse scontrol output")
    if node_count == 0:
        return "inf"


    command = f"squeue -p {queue} -o '%D %b %l %T %R'"
    status, output = subprocess.getstatusoutput(command)
    if status != 0:
        raise subprocess.CalledProcessError(status, command, output)

    node_days = 0
    for line in output.split("\n"):
        # count jobs that are not scheduled for priority reasons only.
        if '(Priority)' not in line and 'RUNNING' not in line:
            continue


        (nodes, gres, time_left, _, _) = line.split(" ")
        gpus = 8
        m = re.search("gres:gpu:(\d+)", gres)
        if m:
            gpus = int(m.group(1))

        nodes = int(nodes) * gpus / 8.0
        days = parse_time(time_left) / 86400
        node_days += nodes * days

    return f"{node_days / node_count:.1f}"
