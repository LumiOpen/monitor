import datetime
import json
import os
import requests
import time

from slurmmonitor.checks import check_job_status, check_free_inodes, check_free_bytes
from slurmmonitor.config import job_config, free_bytes_config, free_inodes_config
from slurmmonitor.snapshot import ClusterDataSnapshot
from slurmmonitor.message import MessageTracker

from dotenv import load_dotenv

load_dotenv()

def post_msg(message):
    print(message)
    headers = {'Content-Type': 'application/json'}
    payload = {'text': message}

    webhook_url = os.getenv("WEBHOOK_URL")
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        print(f"Error posting to slack: {response.status_code}, {response.text}")

def main():
    message_tracker = MessageTracker()

    last_time = datetime.datetime.now()
    prev_snapshot = None
    snapshot = None
    first_run = True
    while True:
        prev_snapshot = snapshot
        snapshot = ClusterDataSnapshot()

        messages = []
        messages.extend(check_free_bytes(free_bytes_config, snapshot))
        messages.extend(check_free_inodes(free_inodes_config, snapshot))
        messages.extend(check_job_status(job_config, snapshot, prev_snapshot))

        out_messages = []
        for message in messages:
            out_message = message_tracker.handle(message)
            if out_message is not None:
                out_messages.append(out_message)
        out_message = "\n".join([str(i) for i in out_messages])
        if out_message:
            if first_run:
                first_run = False
                print(out_message)
                continue
            post_msg(out_message)

        with open("log.jsonl", "a") as f:
            f.write(json.dumps({
                "timestamp": time.time(),
                "job_state": {k: v.model_dump() for k,v in snapshot.jobs.items() if v is not None},
            }) + "\n")

        current_time = datetime.datetime.now()
        if last_time.hour == 8 and current_time.hour == 9:
            active_messages = message_tracker.get_active_messages()
            daily_message = "\n".join([str(i) for i in active_messages])
            post_msg("Daily Status:\n" + daily_message)
        last_time = current_time

        time.sleep(60)


if __name__ == "__main__":
    main()
