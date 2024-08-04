from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pandas_filter_bike_dag',
    default_args=default_args,
    description='A DAG to filter bike data using Pandas and push DataFrame to XCom',
    schedule_interval=timedelta(days=1),
)

input_path = os.path.join(os.path.dirname(__file__), 'BIKE DETAILS.csv')

def filter_bike_data(**kwargs):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(input_path)
    
    # Filter the data (for example, only include bikes with year more than 2015)
    filtered_df = df[df['year'] > 2015]
    
    # Serialize the DataFrame to JSON format
    filtered_df_json = filtered_df.to_json(orient='split')
    
    # Push the JSON data to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='filtered_df_json', value=filtered_df_json)

t1 = PythonOperator(
    task_id='filter_bike_data',
    python_callable=filter_bike_data,
    provide_context=True,
    dag=dag,
)

def retrieve_dataframe(**kwargs):
    ti = kwargs['ti']
    filtered_df_json = ti.xcom_pull(task_ids='filter_bike_data', key='filtered_df_json')
    
    # Deserialize the JSON data back to a DataFrame
    filtered_df = pd.read_json(filtered_df_json, orient='split')
    
    # Print DataFrame or use it as needed
    print(filtered_df.head())

t2 = PythonOperator(
    task_id='retrieve_dataframe',
    python_callable=retrieve_dataframe,
    provide_context=True,
    dag=dag,
)

t1 >> t2