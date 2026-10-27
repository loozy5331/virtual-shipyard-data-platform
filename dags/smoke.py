"""빈 DAG(0단계 완료 기준). 스케줄러·실행기·DAG 처리기가 함께 도는지만 본다."""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(schedule=None, catchup=False, tags=["smoke"])
def smoke():
    EmptyOperator(task_id="noop")


smoke()
