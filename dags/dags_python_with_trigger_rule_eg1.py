from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import pendulum

with DAG (
    dag_id ='dags_python_with_trigger_rule_eg1',
    schedule=None,
    start_date=pendulum.datetime(2025,7,1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    bash_upstream_1=BashOperator(
        task_id='bash_upstream_1',
        bash_command= "echo upstream"
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstram_1 Exception!')


    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상 처리')

    @task(task_id='python_downstram_1',trigger_rule='all_done')
    def python_downstream_1():
        print('정상 처리')

    [bash_upstream_1,python_upstream_1(),python_upstream_2()] >> python_downstream_1()


    