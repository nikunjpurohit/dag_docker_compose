from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json

default_args = {
    'owner': '01_airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 6),
    'retries': 5,
}

dag = DAG(
    '01_data_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(seconds=15),
)

def get_data():
    
    res =  requests.get('https://randomuser.me/api/')
    res= res.json()['results'][0]
    print("Hello, I am here Airflow!")
    #print(json.dumps(res.json()['results'][0], indent=3))
    return res

def format_data(res):
    data ={}
    location = res['location']
    data['first_name']= res['name']['first']
    data['last_name']= res['name']['last']
    data['gender']= res['gender']
    data['address']= f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode']= location['postcode']
    data['email']= res['email']
    data['username']= res['login']['username']
    data['dob']= res['dob']['date']
    data['registered_date']= res['registered']['date']
    data['phone']= res['phone']
    data['picture']= res['picture']['medium']
    print(data)
    return data

def stream_data():
    res=get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))

    

def print_hello():
    print("Hello, I am here Airflow!")

hello_task_1 = PythonOperator(
    task_id='hello_task_1',
    python_callable=print_hello,
    dag=dag,
)

hello_task_2 = PythonOperator(
    task_id='hello_task_2',
    python_callable=stream_data,
    dag=dag,
)
stream_data()
hello_task_1 >> hello_task_2
