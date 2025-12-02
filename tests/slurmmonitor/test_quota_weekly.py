from datetime import datetime, timedelta

from slurmmonitor.quota import compute_gpu_quota_messages


def _make_cfg(updated_at: datetime, days_remaining: int):
    start_dt = updated_at - timedelta(days=100)
    end_dt = updated_at + timedelta(days=days_remaining)
    return {
        "project_462000963": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d"),
        }
    }


def test_quota_weekly_target_and_eta(monkeypatch):
    now = datetime.now()
    updated_at = now
    days_left = 227

    def fake_get_allocs():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 172_200,
                    "gpu_allocated": 1_500_000,
                }
            },
        }

    # 2925 days at 2 GPUs => 70200 GPUh (divide by 2 for MI250X => 1x multiplier)
    sacct = (
        "Account|User|Elapsed|AllocTRES|Start\n"
        "project_462000963|alice|2925-00:00:00|gres/gpu=2|2025-10-01T00:00:00\n"
    )

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_allocs)
    monkeypatch.setattr("slurmmonitor.quota.run_or_raise", lambda cmd: sacct)

    cfg = _make_cfg(updated_at, days_left)
    lines = compute_gpu_quota_messages(cfg)
    assert len(lines) == 1
    line = lines[0]

    # Base used/alloc and remaining days
    assert "used 172.2K/1500.0K GPUh (11.5%)" in line

    # Weekly usage and percent of target
    assert "last 7d 70.2K GPUh (171% of 40.9K GPUh target)" in line

    # Target 7d to finish on time and ETA
    assert "ETA ~132d/227d" in line


def test_quota_weekly_no_output_hides_weekly_and_target(monkeypatch):
    now = datetime.now()
    updated_at = now

    def fake_get_allocs():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 10_000,
                    "gpu_allocated": 100_000,
                }
            },
        }

    # Header only -> no rows for the account
    sacct = "Account|User|Elapsed|AllocTRES|Start\n"

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_allocs)
    monkeypatch.setattr("slurmmonitor.quota.run_or_raise", lambda cmd: sacct)

    cfg = _make_cfg(updated_at, 30)
    lines = compute_gpu_quota_messages(cfg)
    assert len(lines) == 1
    line = lines[0]

    assert "last 7d" not in line
    assert "target 7d" not in line
    assert "ETA ~" not in line


def test_quota_weekly_expired_project_no_weekly(monkeypatch):
    now = datetime.now()

    def fake_get_allocs():
        return {
            "updated_at": now,
            "projects": {
                "project_462000963": {
                    "gpu_used": 50_000,
                    "gpu_allocated": 100_000,
                }
            },
        }

    sacct = (
        "Account|User|Elapsed|AllocTRES|Start\n"
        "project_462000963|bob|10-00:00:00|gres/gpu:mi250x=4|2025-10-01T00:00:00\n"
    )

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_allocs)
    monkeypatch.setattr("slurmmonitor.quota.run_or_raise", lambda cmd: sacct)

    # End already passed
    cfg = {
        "project_462000963": {
            "start": "2000-01-01",
            "end": "2000-12-31",
        }
    }

    lines = compute_gpu_quota_messages(cfg)
    assert len(lines) == 1
    line = lines[0]
    assert "allocation period ended on 2000-12-31" in line
    assert "last 7d" not in line
    assert "target 7d" not in line
    assert "ETA ~" not in line


def test_quota_weekly_sacct_failure(monkeypatch):
    now = datetime.now()

    def fake_get_allocs():
        return {
            "updated_at": now,
            "projects": {
                "project_462000963": {
                    "gpu_used": 1_000,
                    "gpu_allocated": 10_000,
                }
            },
        }

    def fail_run(cmd):
        raise RuntimeError("sacct failed")

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_allocs)
    monkeypatch.setattr("slurmmonitor.quota.run_or_raise", fail_run)

    cfg = _make_cfg(now, 10)
    lines = compute_gpu_quota_messages(cfg)
    assert len(lines) == 1
    line = lines[0]
    assert "last 7d" not in line
    assert "target 7d" not in line
    assert "ETA ~" not in line
