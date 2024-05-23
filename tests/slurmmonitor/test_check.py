import pytest
from slurmmonitor.message import Message
from slurmmonitor.checks import check_free_bytes, check_free_inodes, check_job_status

class MockClusterState:
    def __init__(self, free_bytes=None, free_inodes=None, jobs=None):
        self.free_bytes = free_bytes or {}
        self.free_inodes = free_inodes or {}
        self.jobs = jobs or {}

class MockJob:
    def __init__(self, name, running=True, state="RUNNING", job_id=4, emoji="😊", time_left=3600, logfile="/tmp/log.txt", stalled=False):
        self.name = name
        self.running = running
        self.state = state
        self.job_id = job_id
        self.emoji = emoji
        self.time_left = time_left
        self.logfile = logfile
        self.stalled_status = stalled

    def stalled(self):
        return self.stalled_status

    def progress(self):
        return "50%"

def test_check_free_bytes():
    cluster_state = MockClusterState(
        free_bytes={
            "/path1": 100,
            "/path2": 50
        }
    )
    
    free_bytes_config = {
        "/path1": 80,
        "/path2": 60
    }
    
    messages = check_free_bytes(free_bytes_config, cluster_state)
    
    assert len(messages) == 2
    assert messages[0].topic == "free_bytes /path1"
    assert messages[0].text == "Sufficient free space on /path1"
    assert messages[0].details == "100 > 80"
    assert not messages[0].active
    
    assert messages[1].topic == "free_bytes /path2"
    assert messages[1].text == "Not enough free space on /path2"
    assert messages[1].details == "50 < 60"
    assert messages[1].active

def test_check_free_inodes():
    cluster_state = MockClusterState(
        free_inodes={
            "/path1": 1000,
            "/path2": 500
        }
    )
    
    free_inodes_config = {
        "/path1": 800,
        "/path2": 600
    }
    
    messages = check_free_inodes(free_inodes_config, cluster_state)
    
    assert len(messages) == 2
    assert messages[0].topic == "free_inodes /path1"
    assert messages[0].text == "Sufficient free inodes on /path1"
    assert messages[0].details == "1000 > 800"
    assert not messages[0].active
    
    assert messages[1].topic == "free_inodes /path2"
    assert messages[1].text == "Not enough free inodes on /path2"
    assert messages[1].details == "500 < 600"
    assert messages[1].active

def test_check_job_status_job_state_changed():
    job1 = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
    )
    
    prev_job1 = MockJob(
        name="job1",
        running=False,
        state="PENDING",
    )

    cluster_state = MockClusterState(
        jobs={"job1": job1}
    )
    
    prev_cluster_state = MockClusterState(
        jobs={"job1": prev_job1}
    )
    
    job_config = [job1]
    
    messages = check_job_status(job_config, cluster_state, prev_cluster_state)
    
    assert len(messages) == 2
    assert messages[0].topic == "job_status job1"
    assert "job1" in messages[0].text
    assert "RUNNING" in messages[0].text
    assert "Job changed state from PENDING" in messages[0].details
    
    assert messages[1].topic == "job_stalled job1"
    assert messages[1].text == "job1 is not stalled"
    assert not messages[1].active


def test_check_job_status_no_prev_state():
    job1 = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
    )

    cluster_state = MockClusterState(
        jobs={"job1": job1}
    )

    job_config = [job1]

    messages = check_job_status(job_config, cluster_state, None)

    assert len(messages) == 2
    assert messages[0].topic == "job_status job1"
    assert "job1" in messages[0].text
    assert "RUNNING" in messages[0].text
    assert "New job detected" in messages[0].details
    assert messages[0].active
    
    assert messages[1].topic == "job_stalled job1"
    assert messages[1].text == "job1 is not stalled"
    assert not messages[1].active


def test_check_job_status_not_scheduled():
    job1 = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
    )

    cluster_state = MockClusterState(
        jobs={}
    )

    prev_cluster_state = MockClusterState(
        jobs={}
    )

    job_config = [job1]

    messages = check_job_status(job_config, cluster_state, prev_cluster_state)

    assert len(messages) == 1
    assert messages[0].topic == "job_status job1"
    assert "job1" in messages[0].text
    assert messages[0].active
    assert "is not scheduled" in messages[0].text


# stall status checks
def test_check_job_status_no_stalls():
    job1 = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
        stalled=False,
    )
    cluster_state = MockClusterState(
        jobs={"job1": job1}
    )

    job_config = [job1]

    messages = check_job_status(job_config, cluster_state, cluster_state)

    assert len(messages) == 2
    assert messages[0].topic == "job_status job1"
    assert "job1" in messages[0].text
    assert "RUNNING" in messages[0].text
    assert "00d01h00m00s remaining" in messages[0].details
    
    assert messages[1].topic == "job_stalled job1"
    assert messages[1].text == "job1 is not stalled"
    assert messages[1].active == False
    assert "log: /tmp/log.txt" in messages[1].details

def test_check_job_status_stalled_job():
    job1_stalled = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
        stalled=True,
    )
    cluster_state = MockClusterState(
        jobs={"job1": job1_stalled}
    )

    job1 = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
        stalled=False,
    )
    prev_cluster_state = MockClusterState(
        jobs={"job1": job1}
    )

    job_config = [job1_stalled]

    messages = check_job_status(job_config, cluster_state, prev_cluster_state)

    assert len(messages) == 2
    assert messages[0].topic == "job_status job1"
    assert "job1" in messages[0].text
    assert "RUNNING" in messages[0].text
    assert "00d01h00m00s remaining" in messages[0].details
    
    assert messages[1].topic == "job_stalled job1"
    assert messages[1].text == "job1 looks to be stalled"
    assert messages[1].active == True
    assert "log: /tmp/log.txt" in messages[1].details

def test_check_job_status_stalled_job_unstalled():
    job1_stalled = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
        stalled=True,
    )
    prev_cluster_state = MockClusterState(
        jobs={"job1": job1_stalled}
    )

    job1 = MockJob(
        name="job1",
        running=True,
        state="RUNNING",
        stalled=False,
    )
    cluster_state = MockClusterState(
        jobs={"job1": job1}
    )

    job_config = [job1]

    messages = check_job_status(job_config, cluster_state, prev_cluster_state)

    assert len(messages) == 2
    assert messages[0].topic == "job_status job1"
    assert "job1" in messages[0].text
    assert "RUNNING" in messages[0].text
    assert "00d01h00m00s remaining" in messages[0].details
    
    assert messages[1].topic == "job_stalled job1"
    assert messages[1].text == "job1 is not stalled"
    assert messages[1].active == False
    assert "log: /tmp/log.txt" in messages[1].details

if __name__ == "__main__":
    pytest.main()