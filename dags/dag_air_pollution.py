from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from common.api_crawling import get_data
from airflow.models import Variable


var_value=Variable.get("gonggong_api_key")


with DAG(
    dag_id="dag_air_pollution", ## airflow에들어왔을때 보이는 dag이름
    schedule="*/10 * * * *", ## 매일 매시간 10분마다 정보 수집 {분 시 일 월 요일}
    start_date=pendulum.datetime(2023, 9, 20, tz="Asia/Seoul"), ## 서울로설정
    catchup=False ## 날짜 누락된 구간은 코드 실행x(start_date부터 어제까지의 구간은 코드실행X)
    

) as dag:
    ## air pollution api crawling

    air_pollution_api=PythonOperator(
        task_id='air_pollution_api',
        python_callable=get_data,
        op_args=[var_value]
    )
    
    ## 이메일 보내는 오퍼레이터
    send_email_task=EmailOperator(
        task_id='send_email_task',
        to='audwls7857@naver.com', ## 네이버로 보내기
        subject='airflow 작업 결과 전달',
        html_content='Airflow로 대기오염 api수집 성공'
    )
    air_pollution_api >> send_email_task