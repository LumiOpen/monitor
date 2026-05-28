from slurmmonitor.slurm.util import get_queue_days, parse_gres_gpu_count, parse_time, parse_job_state

def test_parse_time_left():
    assert parse_time('1-00:00:00') == 86400
    assert parse_time('2-12:00:00') == 216000
    assert parse_time('0+01:00:00') == 3600
    assert parse_time('00:01:00') == 60
    assert parse_time('01:00:00') == 3600
    assert parse_time('00:00:01') == 1
    assert parse_time('00:00') == 0
    assert parse_time('INVALID') == 0
    assert parse_time('N/A') == 0


def test_parse_gres_gpu_count():
    assert parse_gres_gpu_count('gres/gpu:1') == 1
    assert parse_gres_gpu_count('gres/gpu:8') == 8
    assert parse_gres_gpu_count('gres/gpu:mi250:8') == 8
    assert parse_gres_gpu_count('gres:gpu:4') == 4
    assert parse_gres_gpu_count('N/A') == 8


def test_get_queue_days_parses_live_squeue_formats(monkeypatch):
    outputs = [
        'PartitionName=standard-g TotalNodes=4 State=UP',
        '\n'.join([
            'NODES TRES_PER_NODE TIME_LIMIT STATE NODELIST(REASON)',
            '1 gres/gpu:1 1-00:00:00 PENDING (Priority)',
            '1 gres/gpu:mi250:8 12:00:00 RUNNING nid000001',
            '1 gres/gpu:2 INVALID PENDING (Priority)',
            '1 gres/gpu:8 1-00:00:00 PENDING (Dependency)',
        ]),
    ]

    monkeypatch.setattr('slurmmonitor.slurm.util.run_or_raise', lambda _: outputs.pop(0))

    assert get_queue_days('standard-g') == '0.2'


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
