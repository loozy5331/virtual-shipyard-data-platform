from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 계획의 폴더 구조. 빠지면 이후 단계의 경로가 어긋난다.
FOLDERS = [
    "ingest",
    "dbt",
    "dags",
    "reconcile",
    "stream",
    "scoreboard",
    "contracts",
    "docs/adr",
    "tests",
]


def test_folders_exist():
    missing = [f for f in FOLDERS if not (ROOT / f).is_dir()]
    assert missing == []
