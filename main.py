import argparse
import datetime
import json
import logging
import os
import requests
import time

from slurmmonitor.checks import check_job_status, check_free_inodes, check_free_bytes, check_queue_days
from slurmmonitor.config import job_config, free_bytes_config, free_inodes_config, gpu_quota_projects
from slurmmonitor.snapshot import ClusterDataSnapshot
from slurmmonitor.message import MessageTracker
from slurmmonitor.quota import compute_gpu_quota_messages

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

def setup_logging(debug):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    logger.handlers = []
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(console_handler)


def main(args):
    setup_logging(args.debug)

    message_tracker = MessageTracker()

    last_time = datetime.datetime.now()
    prev_snapshot = None
    snapshot = None
    first_run = True
    while True:
        try:
            new_snapshot = ClusterDataSnapshot()
        except Exception as e:
            print(f"got exception getting ClusterDataSnapshot: {e}")
            time.sleep(5)
            continue
        prev_snapshot = snapshot
        snapshot = new_snapshot

        messages = []
        messages.extend(check_queue_days(snapshot))
        messages.extend(check_free_bytes(free_bytes_config, snapshot))
        messages.extend(check_free_inodes(free_inodes_config, snapshot))
        messages.extend(check_job_status(job_config, snapshot, prev_snapshot))

        out_messages = []
        for message in messages:
            out_message = message_tracker.handle(message)
            if out_message is not None:
                out_messages.append(out_message)
        out_message = "\n".join([str(i) for i in out_messages])
        # Include GPU quota in first loop output to aid local runs
        if first_run:
            try:
                quota_lines = compute_gpu_quota_messages(gpu_quota_projects)
                if quota_lines:
                    if out_message:
                        out_message += "\n"
                    out_message += "\n".join(quota_lines)
            except Exception as e:
                print(f"Error computing GPU quota messages: {e}")
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

            # Append GPU quota status (daily only)
            try:
                quota_lines = compute_gpu_quota_messages(gpu_quota_projects)
                if quota_lines:
                    if daily_message:
                        daily_message += "\n"
                    daily_message += "\n".join(quota_lines)
            except Exception as e:
                print(f"Error computing GPU quota messages: {e}")
            post_msg("Daily Status:\n" + daily_message)
        last_time = current_time

        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    main(args)
