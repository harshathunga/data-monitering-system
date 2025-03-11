# RUN pip install -r requirements.txt

# FROM apache/airflow:latest

# USER root
# RUN apt-get update && \apt-get -y install git && \ apt-get clean

# USER airflow

# RUN pip install mysql-connector-python
RUN pip install pandas
