import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Top 10 domain zones
def get_top_10_domain_zones():
    df_top_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_top_10_domain_zones = df_top_data['domain'].str.extract(r'.+(?P<domain_zone>\.\w+)')\
        .value_counts() \
        .sort_values(ascending=False) \
        .reset_index() \
        .rename(columns={0:'count'}) \
        .head(10)
    
    with open('TOP_10_DOMAIN_ZONES', 'w') as f:
        f.write(df_top_10_domain_zones.to_csv(index=False, header=False))

# Longest domain name
def get_longest_domain_name():
    df_top_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    df_top_data['domain_len'] = df_top_data['domain'].str.len()
    max_domain_len = df_top_data['domain_len'].max()
    
    longest_domain_name = df_top_data[df_top_data['domain_len'] == max_domain_len] \
        .sort_values('domain') \
        .iloc[0]['domain']
    
    with open('LONGEST_DOMAIN_NAME', 'w') as f:
        f.write(longest_domain_name)

# Airflow.com rank
def get_airflow_rank():
    df_top_data = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_airflow = df_top_data[df_top_data['domain'] == 'airflow.com']
    
    airflow_rank = 'not found'
    
    if df_airflow.shape[0] != 0:
        airflow_rank = df_airflow['rank'].iloc[0]
    
    with open('AIRFLOW_RANK', 'w') as f:
        f.write(airflow_rank)
        
        
def print_data(ds): 
    with open('TOP_10_DOMAIN_ZONES', 'r') as f:
        top_10_domain_zones = f.read()
    
    with open('LONGEST_DOMAIN_NAME', 'r') as f:
        longest_domain_name = f.read()
        
    with open('AIRFLOW_RANK', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'''Report for the date {date}
              1. Top 10 domain zones:
              {top_10_domain_zones}
              2. Longest domain name is {longest_domain_name}
              3. Airflow.com rank is {airflow_rank}''')
    
default_args = {
    'owner': 'v-klimuk-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': '0 10 * * *'
}
dag = DAG('v-klimuk-22-task-1', default_args=default_args)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain_name',
                        python_callable=get_longest_domain_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5