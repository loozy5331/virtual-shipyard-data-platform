"""레포 폴더 구조를 지키는 테스트. 구조는 README "구조"와 계획 0단계를 따른다."""

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
    """계획의 폴더가 모두 있다.

    빈 폴더는 .gitkeep으로 남긴다. 폴더를 옮기거나 지우면 compose의 마운트(airflow/dags,
    infra/)와 이후 단계의 경로가 어긋나므로, 구조를 바꿀 때는 README와 여기를 함께 고친다.
    """
    missing = [f for f in FOLDERS if not (ROOT / f).is_dir()]
    assert missing == []
