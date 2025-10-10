from datetime import datetime

from slurmmonitor.lumi.allocations import parse_lumi_allocations


SAMPLE = "> lumi-allocations\n" \
         "Data updated: 2025-10-10 12:05:41\n" \
         "Project             |                    CPU (used/allocated)|               GPU (used/allocated)|           Storage (used/allocated)\n" \
         "----------------------------------------------------------------------\n" \
         "project_462000353   |            657004/0    (N/A) core/hours|       9337612/0    (N/A) gpu/hours|        6560216/0    (N/A) TB/hours\n" \
         "project_462000963   |      174875/1500000  (11.7%) core/hours|  125563/1500000   (8.4%) gpu/hours|   908606/8000000  (11.4%) TB/hours\n"


def test_parse_lumi_allocations_basic():
    data = parse_lumi_allocations(SAMPLE)
    assert data["updated_at"] == datetime(2025, 10, 10, 12, 5, 41)

    projects = data["projects"]
    assert "project_462000353" in projects
    assert "project_462000963" in projects

    p353 = projects["project_462000353"]
    assert p353["gpu_used"] == 9_337_612
    assert p353["gpu_allocated"] == 0

    p963 = projects["project_462000963"]
    assert p963["gpu_used"] == 125_563
    assert p963["gpu_allocated"] == 1_500_000
