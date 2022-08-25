import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'v-klimuk-22'
target_year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'v-klimuk-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 24),
    'schedule_interval': '0 10 * * *'
}

CHAT_ID = -740789660
BOT_TOKEN = Variable.get('telegram_secret')

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Congrats! Dag {dag_id} completed on {date}.'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass
    
@dag(default_args=default_args, catchup=False)
def v_klimuk_22_task_2():
    
    @task()
    def get_data():
        df = pd.read_csv(path_to_file)
        df = df[df['Year'] == target_year]
        return df
    
    # Which game is worldwide bestseller this year?
    @task()
    def top_game_global(df):
        top_game_global = df.groupby('Name', as_index=False) \
            .agg({'Global_Sales': 'sum'}) \
            .sort_values('Global_Sales', ascending=False) \
            .iloc[0]['Name']
        return top_game_global
    
    # Which game genre is Europe bestseller? 
    # List all if there is more than one.
    @task()
    def top_genre_eu(df):
        df_genre_eu = df.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        max_sales_eu = df_genre_eu['EU_Sales'].max()
        top_genre_eu = df_genre_eu[df_genre_eu['EU_Sales'] == max_sales_eu]['Genre'].to_list()
        return top_genre_eu
    
    # On which platform in North America the most games with more than 1 million copies are sold? 
    # List all if there is more than one.
    @task()
    def top_platform_na(df):
        df_1m_sales_na = df[df['NA_Sales'] > 1]
        df_platform_na = df_1m_sales_na.groupby('Platform', as_index=False) \
            .agg({'Name': 'count'}) \
            .rename(columns={'Name': 'Count'})
        max_count_na = df_platform_na['Count'].max()
        top_platform_na = df_platform_na[df_platform_na['Count'] == max_count_na]['Platform'].to_list()
        return top_platform_na
    
    # Which publisher in Japan has the highest average sales? 
    # List all if there is more than one.
    @task()
    def top_publisher_jp(df):
        df_top_publisher_jp = df.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'})
        max_avg_sales_jp = df_top_publisher_jp['JP_Sales'].max()
        top_publisher_jp = df_top_publisher_jp[df_top_publisher_jp['JP_Sales'] == max_avg_sales_jp]['Publisher'].to_list()
        return top_publisher_jp
    
    # How many games are sold better in Europe rather than in Japan?
    @task()
    def sales_EU_more_than_JP(df):
        df_sales_eu_jp = df.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        sales_EU_more_than_JP = (df_sales_eu_jp['EU_Sales'] > df_sales_eu_jp['JP_Sales']).sum()
        return sales_EU_more_than_JP
    
    @task(on_success_callback=send_message)
    def print_data(top_game_global,
                   top_genre_eu,
                   top_platform_na,
                   top_publisher_jp,
                   sales_EU_more_than_JP):
        
        context = get_current_context()
        date = context['ds']
        
        print(f'''{target_year} analysis for the date {date}
                  1. Which game is worldwide bestseller this year? 
                  {top_game_global}
                  2. Which game genre is Europe bestseller? 
                  {top_genre_eu}
                  3. On which platform in North America the most games with more than 1 million copies are sold?  
                  {top_platform_na}
                  4. Which publisher in Japan has the highest average sales? 
                  {top_publisher_jp}
                  5. How many games are sold better in Europe rather than in Japan?
                  {sales_EU_more_than_JP}''')
        
        
    df = get_data()
    top_game_global = top_game_global(df)
    top_genre_eu = top_genre_eu(df)
    top_platform_na = top_platform_na(df)
    top_publisher_jp = top_publisher_jp(df)
    sales_EU_more_than_JP = sales_EU_more_than_JP(df)
    print_data(top_game_global,
               top_genre_eu,
               top_platform_na,
               top_publisher_jp,
               sales_EU_more_than_JP)  
    
v_klimuk_22_task_2 = v_klimuk_22_task_2() 