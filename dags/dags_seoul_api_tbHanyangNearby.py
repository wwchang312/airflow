from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_seoul_api_tbHanyangNearby',
    schedule= None,
    start_date=pendulum.datetime(2025,7,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    '''서울시 한양도성 주변유적지 정보 '''
    tb_HanyangNearby_info = SeoulApiToCsvOperator(
        task_id='tb_HanyangNearby_info',
        dataset_nm='tbHanyangNearby',
        path='/opt/airflow/files/tbHanyangNearby/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}', #
        file_name='tbHanyangNearby.csv'
    )

    tb_HanyangNearby_info