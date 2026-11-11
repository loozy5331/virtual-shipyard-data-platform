"""feat/bronze: dt 하나의 part-*.jsonl 전부 → bronze Parquet, 다시 돌리면 덮어씀."""

import json
from pathlib import Path

import duckdb
import pytest

from pipelines.ingest.bronze import ingest_dt

DT = "2029-06-01"


def _event(seq: int) -> str:
    """봉투 한 줄. 내용은 bronze가 해석하지 않으니 줄마다 달라 구별만 되면 된다."""
    return json.dumps(
        {
            "event_id": f"00000000-0000-7000-8000-{seq:012d}",
            "event_type": "equipment.down",
            "schema_version": 1,
            "source": "eam",
            "occurred_at": f"{DT}T08:00:00+09:00",
            "emitted_at": f"{DT}T08:00:04+09:00",
            "run_id": "v0-s0",
            "entity_key": "ASM-1",
            "seq": seq,
            "hull_no": None,
            "block_id": None,
            "process": None,
            "resource_id": "ASM-1",
            "payload": {},
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _land(landing: Path, name: str, lines: list[str], dt: str = DT) -> None:
    d = landing / f"dt={dt}"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text("".join(line + "\n" for line in lines), encoding="utf-8")


def _bronze(bronze: Path, dt: str = DT) -> list[tuple[str, str, int]]:
    parquet_files = list((bronze / f"dt={dt}").glob("*.parquet"))
    if not parquet_files:
        return []

    rows = duckdb.sql(
        f"select raw, source_file, line_no from read_parquet('{bronze}/dt={dt}/*.parquet')"
    ).fetchall()
    return sorted(rows, key=lambda r: (r[1], r[2]))


def test_reads_every_part_file_with_lineage(tmp_path):
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    emitted = [_event(1), _event(2), _event(3)]
    replayed = [_event(9)]
    _land(landing, "part-0.jsonl", emitted)
    _land(landing, "part-replay-20291102T090000.jsonl", replayed)

    n = ingest_dt(landing, bronze, DT)

    assert n == 4
    assert _bronze(bronze) == [
        (emitted[0], "part-0.jsonl", 1),
        (emitted[1], "part-0.jsonl", 2),
        (emitted[2], "part-0.jsonl", 3),
        (replayed[0], "part-replay-20291102T090000.jsonl", 1),
    ]


def test_rerun_gives_same_result(tmp_path):
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    _land(landing, "part-0.jsonl", [_event(1), _event(2)])

    ingest_dt(landing, bronze, DT)
    first = _bronze(bronze)
    ingest_dt(landing, bronze, DT)

    assert _bronze(bronze) == first  # 두 배가 되지 않는다


def test_rerun_overwrites_with_current_landing(tmp_path):
    """재발행분이 나중에 들어온 dt를 다시 돌리면, bronze는 지금 착륙 구역과 같아진다."""
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    _land(landing, "part-0.jsonl", [_event(1)])
    ingest_dt(landing, bronze, DT)

    _land(landing, "part-replay-20291102T090000.jsonl", [_event(9)])
    n = ingest_dt(landing, bronze, DT)

    assert n == 2
    assert [r[1] for r in _bronze(bronze)] == [
        "part-0.jsonl",
        "part-replay-20291102T090000.jsonl",
    ]


def test_touches_only_the_given_dt(tmp_path):
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    other = "2029-06-02"
    _land(landing, "part-0.jsonl", [_event(1)])
    _land(landing, "part-0.jsonl", [_event(2), _event(3)], dt=other)

    ingest_dt(landing, bronze, other)
    ingest_dt(landing, bronze, DT)

    assert len(_bronze(bronze, other)) == 2
    assert len(_bronze(bronze)) == 1


def test_empty_landing_dir(tmp_path):
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    n = ingest_dt(landing, bronze, DT)

    assert n == 0
    assert _bronze(bronze) == []
    assert bronze.exists()


def test_invalid_dt(tmp_path):
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    # format
    with pytest.raises(ValueError, match="Invalid isoformat string: '2029-6-1'"):
        ingest_dt(landing, bronze, "2029-6-1")

    with pytest.raises(ValueError, match="dt=20290601 is not in YYYY-MM-DD format"):
        ingest_dt(landing, bronze, "20290601")

    # date value
    with pytest.raises(ValueError, match="day is out of range for month"):
        ingest_dt(landing, bronze, "2029-02-33")


def test_remove_landing_file(tmp_path):
    landing, bronze = tmp_path / "landing", tmp_path / "bronze"
    _land(landing, "part-0.jsonl", [_event(1), _event(2)])
    ingest_dt(landing, bronze, DT)

    # Remove the landing file
    (landing / f"dt={DT}" / "part-0.jsonl").unlink()

    n = ingest_dt(landing, bronze, DT)

    assert n == 0
    assert _bronze(bronze) == []
