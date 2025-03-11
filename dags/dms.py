import requests
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()
import pandas as pd
import smtplib
import time
import pyodbc
import mysql.connector
from airflow import DAG
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pytz

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

def fech_data_and_transform():
    ist = pytz.timezone("Asia/Kolkata")

    # Get current time in IST
    current_time_ist = datetime.now(ist)
    current_time_ist = current_time_ist.strftime("%Y-%m-%d %H:%M:%S")

    url = 'https://www.moneycontrol.com/markets/indian-indices/'



    data = requests.get(url)
    soup = BeautifulSoup(data.text, 'html.parser')
    # print (soup)
    # return soup


    ltp = soup.find_all('tr')

    data = []

    # print(len(ltp))
    # print(ltp.text)

    for i in ltp:

        row = i.text
        # print(row)
        data.append(row)


    processed_data = list(map(lambda x: x.strip().split("\n"), data))
    pd.set_option("display.max_rows", None)

    # Show all columns
    pd.set_option("display.max_columns", None)

    # columns = {'Stock Name', 'Sector'}
    c = pd.DataFrame(processed_data, columns = ["stock_name", "sector",'ltp', 'change','%cng' ])
    processed_data = list(map(lambda x: x.strip().split("\n"), data))
    pd.set_option("display.max_rows", None)

    # Show all columns
    pd.set_option("display.max_columns", None)


    c = pd.DataFrame(processed_data, columns = ["stock_name", "sector",'ltp', 'change','%cng' ])
    # print(c
    newdata = c.dropna()
    newdata['date&time'] = current_time_ist

    o = newdata[newdata['stock_name'] == 'Stock Name'].index

    newdata.drop(o, inplace = True)
    newdata = newdata.reset_index()
    newdata['ltp'] = newdata['ltp'].str.replace(",", "").astype(float)
    newdata[['change', '%cng']] = newdata[['change', '%cng']].replace(",", "").astype(float)

    newdata['date&time'] = newdata['date&time'].astype('datetime64[ns]')

    newdata

    

   
    line = mysql.connector.connect(

        host="172.18.0.1", 
        port= 3307,
        # host="localhost",         
        user="root",       # give your sql userid
        password= '',   # give your sql password
        database="test"    #your database name   
    )
    cursor = line.cursor()

    print(cursor)
    cursor = line.cursor()


    cursor.execute('''create table if not exists mstocks (
             
                   
            stock_name varchar(100),
            sector varchar(100),
            ltp float(50),
            changes float(50),
            cng float(50),
            dateandtime datetime
               );''')



    for index, i in newdata.iterrows():
        sql = "insert into mstocks(stock_name,sector,ltp,changes,cng,dateandtime) values(%s,%s,%s,%s,%s,%s)"
        val = (i['stock_name'],	i['sector'],i['ltp'],i['change'],i['%cng'],i['date&time'])
        cursor.execute(sql, val)
    # print(val)
    line.commit()

    # Close the connection
    line.close()

    return newdata




def check_nulls(n):
        if n.isnull().any().any() > 0:
            current_time = time.localtime()
            print("there are null")
            
            receiver_email = "xyz@gmail.com"
            subject = "airflow data monitering sysytem"
            body = str("There are null values in the data." + str(current_time))

            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(os.getenv("senders_email"), "fnxk vrba fkxx sbcx")
            s.sendmail(os.getenv("senders_email"), receiver_email, subject, body)

        else:
            print("there no nulls")





def airflow_dags():

    # fech_data_and_transform()
    check_nulls(fech_data_and_transform())
    # store_data(fech_data_and_transform())


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 13),
}

# Initialize the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch real-time stock data and store it in MySQL',
    schedule_interval='@hourly',  # Runs every hour
    catchup=False
)

task = PythonOperator(
    task_id = 'airflow_dags',
    python_callable= airflow_dags,
    dag=dag
) 
# Task dependencies
task