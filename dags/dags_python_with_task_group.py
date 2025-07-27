from airflow import DAG
from airflow.decorators import task,task_group
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG (
    dag_id ='dags_python_with_task_group',
    schedule=None,
    start_date=pendulum.datetime(2025,7,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def inner_func(**kwargs):
        msg=kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        '''task_group 데커레이터를 이용한 첫번째 그룹'''

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫번째 TaskGroup내 첫번째 task 입니다.')
        
        inner_function2=PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg' : '첫번째 TaskGroup내 두번째 Task입니다.'}
        )
        inner_func1() >> inner_function2


    with TaskGroup(group_id='second_group',tooltip='두번째 그룹입니다.') as group_2:
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두번째, TaskGroup 내 첫번째 Task 입니다.')
        
        inner_function2=PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'2번째 TaskGroup내 두번째 Task입니다.'}
        )
        inner_func1() >>inner_function2
    
    group_1() >> group_2