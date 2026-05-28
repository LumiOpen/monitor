import pytest

from slurmmonitor import snapshot


class StatvfsResult:
    def __init__(self, *, blocks=1000, bfree=900, bavail=100, frsize=4096, files=1000, ffree=900, favail=100):
        self.f_blocks = blocks
        self.f_bfree = bfree
        self.f_bavail = bavail
        self.f_frsize = frsize
        self.f_files = files
        self.f_ffree = ffree
        self.f_favail = favail


def test_get_free_bytes_uses_available_blocks(monkeypatch):
    monkeypatch.setattr(snapshot.os, 'statvfs', lambda _: StatvfsResult())

    assert snapshot.get_free_bytes('/scratch/project') == 409600


def test_get_free_inodes_uses_available_inodes(monkeypatch):
    monkeypatch.setattr(snapshot.os, 'statvfs', lambda _: StatvfsResult())

    assert snapshot.get_free_inodes('/scratch/project') == 100


def test_statvfs_retries_suspicious_all_available_blocks(monkeypatch):
    results = [
        StatvfsResult(blocks=1000, bfree=1000, bavail=1000),
        StatvfsResult(blocks=1000, bfree=900, bavail=100),
    ]
    monkeypatch.setattr(snapshot.os, 'statvfs', lambda _: results.pop(0))
    monkeypatch.setattr(snapshot.time, 'sleep', lambda _: None)

    assert snapshot.get_free_bytes('/scratch/project') == 409600


def test_statvfs_rejects_repeated_suspicious_all_available_blocks(monkeypatch):
    monkeypatch.setattr(snapshot.os, 'statvfs', lambda _: StatvfsResult(blocks=1000, bfree=1000, bavail=1000))
    monkeypatch.setattr(snapshot.time, 'sleep', lambda _: None)

    with pytest.raises(RuntimeError, match='suspicious free space'):
        snapshot.get_free_bytes('/scratch/project')
