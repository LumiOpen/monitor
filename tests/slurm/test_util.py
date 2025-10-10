from slurmmonitor.slurm.util import parse_time, parse_job_state

def test_parse_time_left():
    assert parse_time('1-00:00:00') == 86400
    assert parse_time('2-12:00:00') == 216000
    assert parse_time('00:01:00') == 60
    assert parse_time('01:00:00') == 3600
    assert parse_time('00:00:01') == 1
    assert parse_time('00:00') == 0


def squeue_line(job_id, state, name, time_running='0+00:00:00', time_left='0+00:00:00', time_since_submit='2024-01-01T00:00:00'):
    return f'{job_id}  {state}  {name}  {time_running}  {time_left}  {time_since_submit}'

def test_parse_job_state_duplicate_pending_jobs_in_order():
    jobs = parse_job_state("\n".join([
        squeue_line(1, 'PENDING', 'jobname1'),
        squeue_line(2, 'PENDING', 'jobname1'),
    ]))

    assert jobs[0].job_id == 1
    assert jobs[1].job_id == 2

def test_parse_job_state_duplicate_pending_jobs_reverse_order():
    jobs = parse_job_state("\n".join([
        squeue_line(2, 'PENDING', 'jobname1'),
        squeue_line(1, 'PENDING', 'jobname2'),
    ]))
    assert jobs[0].job_id == 1
    assert jobs[1].job_id == 2


def test_parse_job_state_duplicate_running_job_first():
    jobs = parse_job_state("\n".join([
        squeue_line(1, 'PENDING', 'jobname1'),
        squeue_line(2, 'RUNNING', 'jobname1'),
    ]))

    assert jobs[0].job_id == 2
    assert jobs[1].job_id == 1
