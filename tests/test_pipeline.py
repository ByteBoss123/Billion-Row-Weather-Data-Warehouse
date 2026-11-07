"""Runs the real job on a 676-row fixture in GHCN format and checks every rule fires as designed."""
import json, subprocess, sys, tempfile
from pathlib import Path

HERE = Path(__file__).parent


def test_pipeline_on_fixture():
    out = tempfile.mkdtemp()
    p = subprocess.run([sys.executable, str(HERE.parent / "pipeline/ghcn_pipeline.py"), str(HERE / "fixture_src"),
                        str(HERE / "fixture_stations.txt"), out, "2023"], capture_output=True, text=True, check=True)
    r = json.loads(p.stdout[p.stdout.index("{"):])
    assert r["rows"]["source"] == r["rows"]["fact"] + r["rows"]["quarantine"] == 676
    q = r["quarantine_by_reason"]
    assert q["bad_station_id"] == 2 and q["bad_date"] == 2          # injected bad rows
    assert q["failed_noaa_qc"] > 0 and q["missing_value_sentinel"] > 0
    assert r["checks"]["no_orphan_stations"]["value"] == 1           # fixture station missing from the dimension
    assert r["checks"]["no_orphan_stations"]["pass"] is False
    assert r["benchmark"]["same_answer"] is True
