from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 계획의 폴더 구조. 빠지면 이후 단계의 경로가 어긋난다.
FOLDERS = [
    "pipelines/ingest",
    "pipelines/reconcile",
    "pipelines/stream",
    "pipelines/scoreboard",
    "airflow/dags",
    "dbt",
    "contracts",
    "infra",
    "docs/adr",
    "tests",
]


def test_folders_exist():
    missing = [f for f in FOLDERS if not (ROOT / f).is_dir()]
    assert missing == []
