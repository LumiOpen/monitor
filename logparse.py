import json
import argparse
from datetime import datetime, timedelta


def read_log(filename):
    with open(filename, "r") as file:
        for line in file:
            yield json.loads(line)

def calculate_uptime(logs, days):
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)


    job_summary = {}
    for log in logs:
        timestamp = datetime.fromtimestamp(log['timestamp'])
        if start_time <= timestamp <= end_time:
            for job_name, job_data in log['job_state'].items():
                if job_name not in job_summary:
                    job_summary[job_name] = {'running_count': 0, 'total_count': 0}
                job_summary[job_name]['total_count'] += 1
                if job_data['running']:
                    job_summary[job_name]['running_count'] += 1
    for job, data in sorted(job_summary.items()):
        percentage = (data['running_count'] / data['total_count']) * 100 if data['total_count'] > 0 else 0
        print(f"Job: {job}, uptime {percentage:.2f}%")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Total entries from log jsonl')
    parser.add_argument('file', type=str, default="log.jsonl", help="log filename")
    parser.add_argument('--days', type=int, default=14, help="number of days history")

    args = parser.parse_args()
    logs = read_log(args.file)
    calculate_uptime(logs, args.days)

    


