from slurmmonitor.message import Message

def check_free_bytes(free_bytes_config, cluster_state):
    messages = []
    for path, threshold in free_bytes_config.items():
        topic = f"free_bytes {path}"
        free_bytes = cluster_state.free_bytes[path]
        if free_bytes < threshold:
            messages.append(Message(
                topic,
                f"Not enough free space on {path}",
                f"{free_bytes} < {threshold}",
            ))
        else:
            messages.append(Message(
                topic,
                f"Sufficient free space on {path}",
                f"{free_bytes} > {threshold}",
                active=False,
            ))
    return messages


def check_free_inodes(free_inodes_config, cluster_state):
    messages = []
    for path, threshold in free_inodes_config.items():
        topic = f"free_inodes {path}"
        free_inodes = cluster_state.free_inodes[path]
        if free_inodes < threshold:
            messages.append(Message(
                topic,
                f"Not enough free inodes on {path}",
                f"{free_inodes} < {threshold}",
            ))
        else:
            messages.append(Message(
                topic,
                f"Sufficient free inodes on {path}",
                f"{free_inodes} > {threshold}",
                active=False,
            ))
    return messages

def check_queue_days(cluster_state):
    messages = []
    for queue, days in cluster_state.queue_days.items():
        messages.append(Message(
            f"queue_days_{queue}",
            f"{queue} queue status",
            f"{days} days"))
    return messages


def check_job_status(job_config, cluster_state, prev_cluster_state):
    messages = []

    for job in job_config:
        current_job = cluster_state.jobs.get(job.name, None)
        last_job = prev_cluster_state.jobs.get(job.name, None) if prev_cluster_state is not None else None

        topic = f"job_status {job.name}"
        if current_job is None:
            text = f"{job.name} is not scheduled"
            if last_job is None:
                messages.append(Message(topic, text, None))
            else:
                messages.append(Message(topic, text, f"last known state {last_job.state}"))
        else:
            running = "running"
            if not current_job.running:
                running = "queued"

            text = f"{job.name} {current_job.job_id} is {running} {current_job.emoji}"

            progress = get_progress(job_config, job)
            time_left = format_seconds(current_job.time_left)
            if last_job is None:
                messages.append(Message(topic, text, f"New job detected in state {current_job.state}, progress {progress}, {time_left} remaining"))
            elif current_job.state != last_job.state:
                messages.append(Message(
                    topic,
                    text,
                    f"Job changed state from {last_job.state}:{last_job.job_id} to {current_job.state}:{current_job.job_id}, progress {progress}, {time_left} remaining"))
            elif current_job.job_id != last_job.job_id:
                messages.append(Message(
                    topic,
                    text,
                    f"Job changed id from {last_job.job_id} to {current_job.job_id}, progress {progress}, {time_left} remaining"))
            else:
                messages.append(Message(topic, text, f"In state {current_job.state}, progress {progress}, {time_left} remaining"))
                
            # Stall Check
            if current_job.running:
                # TODO stall check actually is handled via job_config job entry which is confusing.
                topic = f"job_stalled {job.name}"
                if job.stalled():
                    messages.append(Message(
                        topic,
                        f"{job.name} looks to be stalled",
                        f"log: {job.logfile}",
                    ))
                else:
                    messages.append(Message(
                        topic,
                        f"{job.name} is not stalled",
                        f"log: {job.logfile}",
                        active=False,
                    ))

    return messages

def format_seconds(seconds):
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    formatted = "%02dd%02dh%02dm%02ds" % (days, hours, minutes, seconds)
    return formatted

# TODO this should probably go into the ClusterState class so we're not calling config directly from here.
def get_progress(job_config, job_state):
    for config in job_config:
        if job_state.name == config.name:
            progress = config.progress()
            return progress
    return ""
