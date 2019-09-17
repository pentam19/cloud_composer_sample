import airflow
from airflow import DAG
from airflow.operators import PythonOperator
import datetime
import logging

def test_job(**kwargs):
    #実行日時の取得
    execution_date = kwargs.get('execution_date').date()
    prev_execution_date = execution_date - datetime.timedelta(days=1)
    print(prev_execution_date)
    print(prev_execution_date.strftime("%Y/%m/%d"))
    logging.debug('This is a debug message')
    logging.info('This is an info message')
    logging.warning('This is a warning message')
    logging.error('This is an error message')
    logging.critical('This is a critical message')

    return True

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    'test_etl', default_args=default_args, schedule_interval='@daily')

test_task = PythonOperator(
    task_id='test_job',
    provide_context=True,
    python_callable=test_job,
    dag=dag
)

if __name__ == "__main__":
    dag.cli()

# $ gcloud composer environments run composer-test-env --location us-central1 \
# backfill -- test_etl -s 2017-1-23 -e 2017-1-25
#
# logs
# 1.
'''
[2019-09-17 12:52:02,202] {logging_mixin.py:84} INFO - 2017-01-22
@-@{"workflow": "test_etl", "task-id": "test_job", "execution-date": "2017-01-23T00:00:00"}
[2019-09-17 12:52:02,202] {logging_mixin.py:84} INFO - 2017/01/22
@-@{"workflow": "test_etl", "task-id": "test_job", "execution-date": "2017-01-23T00:00:00"}
'''
# 2.
'''
[2019-09-17 12:52:06,660] {logging_mixin.py:84} INFO - 2017-01-23
@-@{"workflow": "test_etl", "task-id": "test_job", "execution-date": "2017-01-24T00:00:00"}
[2019-09-17 12:52:06,660] {logging_mixin.py:84} INFO - 2017/01/23
@-@{"workflow": "test_etl", "task-id": "test_job", "execution-date": "2017-01-24T00:00:00"}
'''
# 3.
'''
[2019-09-17 12:52:08,339] {logging_mixin.py:84} INFO - 2017-01-24
@-@{"workflow": "test_etl", "task-id": "test_job", "execution-date": "2017-01-25T00:00:00"}
[2019-09-17 12:52:08,339] {logging_mixin.py:84} INFO - 2017/01/24
@-@{"workflow": "test_etl", "task-id": "test_job", "execution-date": "2017-01-25T00:00:00"}
'''