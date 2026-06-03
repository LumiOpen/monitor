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
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {})

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
    assert "days remain" not in line
    assert "last 7d" not in line
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
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {})

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
    assert "days remain" not in line
    assert "last 7d" not in line
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


def test_quota_message_unmet_milestone(monkeypatch):
    updated_at = datetime(2026, 1, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 300_000,
                    "gpu_allocated": 1_000_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {
        "project_462000963": 35_000,
    })

    cfg = {
        "project_462000963": {
            "start": "2025-01-01",
            "end": "2026-12-31",
            "milestone": {
                "name": "checkpoint",
                "date": "2026-01-31",
                "target_pct": 50.0,
            },
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 2
    assert "used 300.0K/1000.0K GPUh (30.0%)" in lines[0]
    assert (
        "checkpoint 2026-01-31: 300.0K/500.0K GPUh target (50.0%), "
        "7d 35.0K/46.7K GPUh target (75% 🥶), ETA ~40d/30d"
    ) in lines[1]


def test_quota_message_met_milestone_still_reports_until_expired(monkeypatch):
    updated_at = datetime(2026, 1, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 550_000,
                    "gpu_allocated": 1_000_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {})

    cfg = {
        "project_462000963": {
            "start": "2025-01-01",
            "end": "2026-12-31",
            "milestone": {
                "name": "checkpoint",
                "date": "2026-01-31",
                "target_pct": 50.0,
            },
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 2
    assert "checkpoint 2026-01-31: 550.0K/500.0K GPUh target (50.0%) (met)" in lines[1]


def test_quota_message_expired_milestone_is_hidden(monkeypatch):
    updated_at = datetime(2026, 2, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 300_000,
                    "gpu_allocated": 1_000_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {})

    cfg = {
        "project_462000963": {
            "start": "2025-01-01",
            "end": "2026-12-31",
            "milestone": {
                "name": "checkpoint",
                "date": "2026-01-31",
                "target_pct": 50.0,
            },
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 1
    assert "checkpoint" not in lines[0]


def test_quota_message_reports_next_unmet_milestone(monkeypatch):
    updated_at = datetime(2026, 1, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462000963": {
                    "gpu_used": 300_000,
                    "gpu_allocated": 1_000_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {})

    cfg = {
        "project_462000963": {
            "start": "2025-01-01",
            "end": "2026-12-31",
            "milestones": [
                {
                    "name": "first checkpoint",
                    "date": "2026-01-15",
                    "target_pct": 20.0,
                },
                {
                    "name": "second checkpoint",
                    "date": "2026-02-01",
                    "target_pct": 50.0,
                },
            ],
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 2
    assert "first checkpoint" not in lines[1]
    assert "second checkpoint 2026-02-01" in lines[1]


def test_quota_message_protect_milestone_wording(monkeypatch):
    updated_at = datetime(2026, 7, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_465002530": {
                    "gpu_used": 500_000,
                    "gpu_allocated": 1_750_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {
        "project_465002530": 35_000,
    })

    cfg = {
        "project_465002530": {
            "start": "2026-05-29",
            "milestone": {
                "name": "resource cut checkpoint",
                "kind": "protect",
                "date": "2026-08-02",
                "target_pct": 40.0,
            },
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 2
    assert "last 7d 35.0K GPUh" in lines[0]
    assert "of " not in lines[0]
    assert "ETA ~" not in lines[0]
    assert "resource cut checkpoint 2026-08-02: 500.0K/700.0K GPUh cut target (40.0%)" in lines[1]


def test_quota_message_unknown_end_without_milestone(monkeypatch):
    updated_at = datetime(2026, 7, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_465002530": {
                    "gpu_used": 500_000,
                    "gpu_allocated": 1_750_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {
        "project_465002530": 35_000,
    })

    cfg = {
        "project_465002530": {
            "start": "2026-05-29",
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 1
    assert "used 500.0K/1750.0K GPUh (28.6%)" in lines[0]
    assert "last 7d 35.0K GPUh" in lines[0]
    assert "ETA ~" not in lines[0]


def test_quota_message_unlock_milestone_uses_target_base(monkeypatch):
    updated_at = datetime(2026, 8, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462001516": {
                    "gpu_used": 400_000,
                    "gpu_allocated": 1_000_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {
        "project_462001516": 70_000,
    })

    cfg = {
        "project_462001516": {
            "start": "2026-05-29",
            "end": "2027-05-31",
            "milestone": {
                "name": "second-half allocation checkpoint",
                "kind": "unlock",
                "date": "2026-10-31",
                "target_mode": "linear",
                "target_base_gpuh": 2_000_000,
            },
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 2
    assert "used 400.0K/1000.0K GPUh (40.0%)" in lines[0]
    assert "last 7d 70.0K GPUh" in lines[0]
    assert "of " not in lines[0]
    assert "ETA ~" not in lines[0]
    assert (
        "second-half allocation checkpoint 2026-10-31: "
        "400.0K/844.7K GPUh bonus target"
    ) in lines[1]
    assert "linear" not in lines[1]


def test_quota_message_absolute_milestone_target(monkeypatch):
    updated_at = datetime(2026, 8, 1, 12, 0, 0)

    def fake_get_lumi_allocations():
        return {
            "updated_at": updated_at,
            "projects": {
                "project_462001516": {
                    "gpu_used": 400_000,
                    "gpu_allocated": 1_000_000,
                }
            },
        }

    monkeypatch.setattr("slurmmonitor.quota.get_lumi_allocations", fake_get_lumi_allocations)
    monkeypatch.setattr("slurmmonitor.quota.get_weekly_gpu_hours_by_project", lambda projects: {})

    cfg = {
        "project_462001516": {
            "start": "2026-05-29",
            "end": "2027-05-31",
            "milestone": {
                "name": "absolute checkpoint",
                "kind": "unlock",
                "date": "2026-10-31",
                "target_gpuh": 1_000_000,
            },
        }
    }

    lines = compute_gpu_quota_messages(cfg)

    assert len(lines) == 2
    assert "absolute checkpoint 2026-10-31: 400.0K/1000.0K GPUh bonus target" in lines[1]
    assert "(50.0%)" not in lines[1]
