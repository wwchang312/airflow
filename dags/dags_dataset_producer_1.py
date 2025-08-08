import pendulum
from airflow.sdk import DAG ,Asset
from airflow.providers.standard.operators.bash import BashOperator

asset_dags_dataset_producer_1 = Asset("dags_dataset_producer_1")

with DAG(
        dag_id='dags_dataset_producer_1',
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2025, 8, 8, tz='Asia/Seoul'),
        catchup=False,
        tags=['asset','producer']
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[asset_dags_dataset_producer_1],  # Produce할 키 생성  즉 키는 태스크에서 만든다. 리스트형태로 담는다.
        bash_command='echo "producer_1 수행 완료"'
    )