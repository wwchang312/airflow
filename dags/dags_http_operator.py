from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='a',
    schedule=None,
    start_date=pendulum.datetime(2025,7,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    tb_HanyangNearby_info = HttpOperator(   #클래스 호출 및 객체화
        task_id='tb_HanyangNearby_info',
        http_conn_id='openapi.seoul.go.kr',    #airflow connection ID
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbHanyangNearby/1/10/', #데이터를 가져올때, 주소를 제외한 나머지 포인트
        method='GET',
        headers={'Content-Type' : 'application/json',   #json 형태로 가져올 것이다.
                 'charset' : 'utf-8',                   #문자는 utf-8
                 'Accept' : '*/*'
                 }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti=kwargs['ti']
        rslt=ti.xcom_pull(task_ids='tb_HanyangNearby_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_HanyangNearby_info >> python_2()

