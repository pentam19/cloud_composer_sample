import airflow
from airflow import DAG
from airflow.operators import PythonOperator
import datetime
import logging

from google.cloud import bigquery


def test_job01(**kwargs):
    print(datetime.datetime.now())
    execution_date = kwargs.get('execution_date').date()
    print(execution_date)
    prev_execution_date = execution_date - datetime.timedelta(days=1)

    time_str = datetime.datetime.now().strftime("%Y-%m-%d::%H:%M:%S")
    client = bigquery.Client()
    query = """
CREATE TABLE IF NOT EXISTS `{0}.{1}}.test01`
(id INT64, text STRING, execution_date STRING, date_now STRING);
INSERT INTO `{0}.{1}.test01`
VALUES (1, 'test', '{3}', '{4}');
""".format('project', 'dataset', execution_date.strftime("%Y-%m-%d::%H:%M:%S"), time_str)
    print(query)
    query_job = client.query(query)

    results = query_job.result() 

    print(datetime.datetime.now())
    return True


def test_job02(**kwargs):
    print(datetime.datetime.now())
    execution_date = kwargs.get('execution_date').date()
    print(execution_date)
    prev_execution_date = execution_date - datetime.timedelta(days=1)

    time_str = datetime.datetime.now().strftime("%Y-%m-%d::%H:%M:%S")
    client = bigquery.Client()
    query = """
CREATE TABLE IF NOT EXISTS `{0}.{1}.test02`
(id INT64, text STRING, execution_date STRING, date_now STRING);
INSERT INTO `{0}.{1}.test02`
VALUES (2, 'test', '{3}', '{4}');
""".format('project', 'dataset', execution_date.strftime("%Y-%m-%d::%H:%M:%S"), time_str)
    print(query)
    query_job = client.query(query)

    results = query_job.result() 

    print(datetime.datetime.now())
    return True

'''
create environments
$ gcloud composer environments create test-composer --location us-central1 --zone=us-central1-c --machine-type=n1-standard-1 --node-count=3 --disk-size=20GB --python-version 3 --project [projectid]
upload dags
$ gcloud composer environments storage dags import --environment test-composer --location us-central1 --source ./sample_two_bq_task.py --project [projectid]
delete environments
$ gcloud composer environments delete test-composer --location us-central1 --project [projectid]
'''
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG(
    'test_composer',
    default_args=default_args,
    #schedule_interval='@daily')
    schedule_interval='30 7 * * *') # UTC (JST 16:30)

test_job01 = PythonOperator(
    task_id='test_job01',
    provide_context=True,
    python_callable=test_job01,
    dag=dag
)

test_job02 = PythonOperator(
    task_id='test_job02',
    provide_context=True,
    python_callable=test_job02,
    dag=dag
)

test_job01 >> test_job02

if __name__ == "__main__":
    dag.cli()
