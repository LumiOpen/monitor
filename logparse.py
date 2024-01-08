import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

def read_log(filename):
    with open(filename, "r") as file:
        for line in file:
            yield json.loads(line)

def calculate_moving_average(logs, moving_days):
    daily_summary = defaultdict(lambda: defaultdict(lambda: {'running_count': 0, 'total_count': 0}))
    json_output = {}

    # Process logs into daily aggregates
    for log in logs:
        day = datetime.fromtimestamp(log['timestamp']).date()
        for job_name, job_data in log['job_state'].items():
            daily_summary[day][job_name]['total_count'] += 1
            if job_data['running']:
                daily_summary[day][job_name]['running_count'] += 1

    all_days = sorted(daily_summary.keys())

    # Determine the last complete day
    last_complete_day = datetime.fromtimestamp(logs[-1]['timestamp']).date() - timedelta(days=1)

    # Calculate moving average for each day, excluding the last incomplete day and first 'moving_days' days
    for i, day in enumerate(all_days):
        if day > last_complete_day:
            break  # Skip the final incomplete day
        if i < moving_days - 1:
            continue  # Skip the first 'moving_days' days

        window_start_index = max(0, i - moving_days + 1)
        window_days = all_days[window_start_index:i+1]

        # Initialize window summary for running and total counts
        window_summary = defaultdict(lambda: {'running_count': 0, 'total_count': 0})
        for w_day in window_days:
            for job, counts in daily_summary[w_day].items():
                window_summary[job]['running_count'] += counts['running_count']
                window_summary[job]['total_count'] += counts['total_count']

        # Format the daily job uptimes into a dictionary
        job_uptimes = {}
        for job, counts in window_summary.items():
            percentage = (counts['running_count'] / counts['total_count']) * 100 if counts['total_count'] > 0 else 0
            job_uptimes[job] = percentage  # Save as float

        # Add the daily entry to the JSON output
        json_output[str(day)] = job_uptimes

    # Dump the JSON output
    print(json.dumps(json_output, indent=4))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calculate moving uptime average from log jsonl and output as JSON')
    parser.add_argument('file', type=str, help="log filename")
    parser.add_argument('--days', type=int, default=7, help="Number of days for moving average")

    args = parser.parse_args()
    logs = list(read_log(args.file))
    calculate_moving_average(logs, args.days)

