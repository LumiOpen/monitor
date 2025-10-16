from datetime import datetime, timedelta

from slurmmonitor.quota import compute_gpu_quota_messages


def test_quota_message_no_stale(monkeypatch):
    # Arrange controlled timestamps
    now = datetime.now()
    updated_at = now - timedelta(hours=1)

    # Symmetric window so time elapsed is exactly 50.0%
    start_dt = updated_at - timedelta(days=100)
    end_dt = updated_at + timedelta(days=100)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 125_563,
                    "gpu_allocated": 1_500_000,
                }
            },
        }

    # Monkeypatch getter to avoid shell call
    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)

    cfg = {
        "project_462000963": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d"),
        }
    }

    # Act
    lines = compute_gpu_quota_messages(cfg)

    # Assert
    assert len(lines) == 1
    line = lines[0]
    assert "project_462000963" in line
    assert "used 125.6K/1500.0K GPUh (8.4%)" in line
    assert "100 days remain" in line
    assert "stale" not in line


def test_quota_message_with_stale(monkeypatch):
    # Arrange a stale dataset
    now = datetime.now()
    updated_at = now - timedelta(hours=48)

    # Symmetric window around updated_at
    start_dt = updated_at - timedelta(days=100)
    end_dt = updated_at + timedelta(days=100)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 125_563,
                    "gpu_allocated": 1_500_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)

    cfg = {
        "project_462000963": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d"),
        }
    }

    # Act
    lines = compute_gpu_quota_messages(cfg)

    # Assert
    assert len(lines) == 1
    line = lines[0]
    assert "project_462000963" in line
    assert "used 125.6K/1500.0K GPUh (8.4%)" in line
    assert "100 days remain" in line
    assert "stale (48h old)" in line


def test_quota_message_expired_project(monkeypatch):
    # Arrange: end in the past
    def fake_get_lumi_allocations():
        return {
            "updated_at": None,
            "projects": {
                "project_462000963": {
                    "gpu_used": 125_563,
                    "gpu_allocated": 1_500_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)

    cfg = {
        "project_462000963": {
            "start": "2000-01-01",
            "end": "2000-12-31",
        }
    }

    # Act
    lines = compute_gpu_quota_messages(cfg)

    # Assert
    assert len(lines) == 1
    line = lines[0]
    assert "allocation period ended on 2000-12-31" in line
    assert "no allocated GPU hours" not in line


def test_quota_message_zero_allocation_not_expired(monkeypatch):
    # Arrange: zero allocated but still in window
    def fake_get_lumi_allocations():
        return {
            "updated_at": None,
            "projects": {
                "project_462000353": {
                    "gpu_used": 9_337_612,
                    "gpu_allocated": 0,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)

    cfg = {
        "project_462000353": {
            "start": "2000-01-01",
            "end": "2999-12-31",
        }
    }

    # Act
    lines = compute_gpu_quota_messages(cfg)

    # Assert
    assert len(lines) == 1
    line = lines[0]
    assert "no allocated GPU hours" in line
