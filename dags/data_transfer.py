from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import logging

from datetime import datetime, timedelta
import pendulum
from airflow.utils.email import send_email
from email.mime.text import MIMEText
import smtplib



# setting the time as indian standard time. We have to set this if we want to schedule a pipeline 
time_zone = pendulum.timezone("Asia/Kolkata")

def send_alert(context):
    print('Task failed sending an Email')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']
    error_message = str(exception)

    subject = f'Airflow Alert: Task Failure in {dag_id}'
    body = f"""
    <br>Task ID: {task_id}</br>
    <br>DAG ID: {dag_id}</br>
    <br>Execution Date: {execution_date}</br>
    <br>Retries: {No_of_retries}</br>
    <br>Delay_between_retry: {retry_delay}</br>
    <br>Task failed and retries exhausted. Manual intervention required.</br>
    """
    
    # Using Airflow's send_email function for consistency and better integration
    send_email(
        to='gauravnagraleofficial@gmail.com',
        subject=subject,
        html_content=body
    )

    log_failure_to_db(task_id, dag_id, execution_date, error_message)

def send_success_alert(context):
    print('Task succeeded sending a notification')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    success_message = f'Task {task_id} in DAG {dag_id} succeeded on {execution_date}.'
    log_success_to_db(task_id, dag_id, execution_date, success_message)


def log_success_to_db(task_id, dag_id, execution_date, success_message):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_dag_success (task_id, dag_id, execution_date, success_message)
    VALUES (%s, %s, %s, %s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, success_message))


def log_failure_to_db(task_id, dag_id, execution_date, error_message):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO AIR_DAG_fail (task_id, dag_id, execution_date, error_message)
    VALUES (%s, %s, %s, %s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, error_message))

def print_data(**kwargs):
    sql_query1 = ''' 
        select count(*) as dest_row_count
        from opd_count
        where TRUNC(date) = TRUNC(SYSDATE);
    '''

    sql_query2 = ''' 
        select count(hrgnum_puk) as source_row_count
        from hrgt_episode_dtl
        where gnum_isvalid = 1
        and gnum_hospital_code = 22914
        --and TRUNC(gdt_entry_date) = TRUNC(SYSDATE);
    '''
    
    dest_hook_dest = PostgresHook(postgres_conn_id='destination_conn_id', schema='Airflow_destination')
    sour_hook_dest = PostgresHook(postgres_conn_id='aiimsnew_conn', schema='aiimsnew')

    dest_row_count = dest_hook_dest.get_records(sql_query1)[0]
    sour_row_count = sour_hook_dest.get_records(sql_query2)[0]

    logging.info(f"Query result: {dest_row_count}")
    logging.info(f"Query result: {sour_row_count}")

    return dest_row_count,sour_row_count

def print_xcom_result(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='compare_row')
    dest_row_count, sour_row_count = result
    logging.info(f"XCom destination row count: {dest_row_count}")
    logging.info(f"XCom source row count: {sour_row_count}")
    if sour_row_count != dest_row_count:
        send_alert(kwargs)
    else:
        logging.info('Row counts match')
    

# Default arguments for the DAG
default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert,
    'on_success_callback': send_success_alert
}

# Define the DAG
with DAG(
        dag_id="Data_transfer",
        default_args=default_args,
        description="Transferring the data from the source to destination DB",
        schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
        # 5 - Mins , 11-Hours ,* - any day of week ,*- any month,*-any day of week 
        catchup=False
    ) as dag:

    # creating the table source table 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='destination_conn_id',
        sql='''
        CREATE TABLE IF NOT EXISTS OPD_count(
            Date TIMESTAMP NOT NULL,
            OPD_count INT NOT NULL,
            date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''',
        dag = dag
    )

    # transferring the data into a staging area
    # with clause specifies that the format for the output should be saved as CSV 
    # and this data is stored in container running in docker
    # the data will be lost once the container is stopped or removed

    transfer_data = PostgresOperator(
        task_id='transfer_data',
        postgres_conn_id='postgres',
        sql='''
            COPY (SELECT trunc(gdt_entry_date) as Date,
                count(hrgnum_puk) as OPD_count
                FROM hrgt_episode_dtl
                WHERE gnum_isvalid = 1
                AND gnum_hospital_code = 22914
                GROUP BY trunc(gdt_entry_date))
        TO '/tmp/staging_data.csv' WITH CSV;
        ''',
        dag = dag
    )

    #Note: the temp folder is present inside the test-airflow-postgres-1 /bin/bash
    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='destination_conn_id',
        sql='''
        COPY OPD_count (Date, OPD_count)
        FROM '/tmp/staging_data.csv' WITH CSV;
        ''',
        dag = dag
    )


    compare_count = PythonOperator(
        task_id='compare_row',
        provide_context=True,
        python_callable=print_data
    )

    log_xcom_result = PythonOperator(
        task_id='log_xcom_result',
        provide_context=True,
        python_callable=print_xcom_result,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    create_table >> transfer_data >> load_data >> compare_count >> log_xcom_result
