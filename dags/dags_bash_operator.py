from airflow import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_t1 = BashOperator( #객체명
        task_id="bash_t1", #TaskID
        bash_command="echo whoami",
    )
	
    bash_t2 = BashOperator( #객체명
        task_id="bash_t2", #TaskID
        bash_command="echo $HOSTNAME",
    )


bash_t1 >> bash_t2 #bash_t1이 돈 후 bash_t2가 돌 것이다.