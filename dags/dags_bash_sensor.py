import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG

with DAG(
    dag_id ='dags_bash_sensor',
    schedule= "0 7 * * *",
    start_date= pendulum.datetime(2025,8,6,tz="Asia/Seoul"),
    catchup=False
    ) as dag:

    sensor_task_by_poke =BashOperator(
        task_id='sensor_task_by_poke',
        env={'FILE':'/opt/airlfow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command= f''' echo $FILE &&       
                            if [-f $FILE]; then 
                                exit 0
                            else
                                exit 1
                            fi''', #FILE변수 선언 후 -> 해당 경로에 파일이 있는지 여부를 테스트하는 테스트 플래그(-f) 조건절 마무리는 fi로
        poke_interval=30, #30초
        timeout=60*2,
        mode='poke',
        soft_fail=False
    )

    sensor_task_by_reschedule =BashOperator(
        task_id='sensor_task_by_reschedule',
        env={'FILE':'/opt/airlfow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command= f''' echo $FILE &&        
                        if [-f $FILE]; then   
                            exit 0
                        else
                            exit 1
                        fi''',
        poke_interval=60*3, 
        timeout=60*9,
        mode='reschedule',
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        env={'FILE': '/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'},
        bash_command='echo "건수: `cat $FILE | wc -l`"',
    )
    
    [sensor_task_by_poke,sensor_task_by_reschedule] >> bash_task

