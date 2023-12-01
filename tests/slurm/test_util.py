from slurm.util import parse_time

def test_parse_time_left():
    assert parse_time('1-00:00:00') == 86400
    assert parse_time('2-12:00:00') == 216000
    assert parse_time('00:01:00') == 60
    assert parse_time('01:00:00') == 3600
    assert parse_time('00:00:01') == 1
    assert parse_time('00:00') == 0



