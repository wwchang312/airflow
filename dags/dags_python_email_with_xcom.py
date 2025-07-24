from airflow import DAG
from airflow.decorators import task
from airflow.providers.smtp.operators.smtp import EmailOperator
import pendulum

with DAG (
    dag_id= 'dags_python_email_with_xcom',
    schedule= None,
    start_date= pendulum.datetime(2025,7,1,tz="Asia/Seoul"),
    catchup=False
) as dag :
    
    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success','Fail'])


    send_email=EmailOperator(
        task_id='send_eamil',
        to='wanw95@naver.com',
        subject = '{{data_interval_end.in_timezone("Asia/Seoul") | ds}} some logic 처리결과',
        html_content='{{date_interval_end.in_timezone("Asia/Seoul" | ds)}} 처리결과 <br> \ {{ti.xcom_pull(task_ids="something_task")}} 했습니다. <br>'
    )


    some_logic() >> send_eamil