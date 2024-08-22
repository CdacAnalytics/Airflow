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

def send_alert(context, dest_row_count=None, sour_row_count=None):
    print('Task failed or row count mismatch detected. Sending an email alert.')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = str(context.get('execution_date'))
    exception = context.get('exception')
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']
    error_message = str(exception) if exception else "Row count mismatch detected."

    subject = f'Airflow Alert: Task Failure or Row Mismatch in {dag_id}'
    body = f"""
    <br>Task ID: {task_id}</br>
    <br>DAG ID: {dag_id}</br>
    <br>Execution Date: {execution_date}</br>
    <br>Retries: {No_of_retries}</br>
    <br>Delay between retry: {retry_delay}</br>
    <br>Error: {error_message}</br>
    """
    if (dest_row_count is not None and sour_row_count is not None) or (dest_row_count != sour_row_count):
        body += f"""
        <br>Source Row Count: {sour_row_count}</br>
        <br>Destination Row Count: {dest_row_count}</br>
        """

    # Send email using Airflow's send_email function
    send_email(
        to='gauravnagraleofficial@gmail.com',
        subject=subject,
        html_content=body
    )

    # Log the failure to the database
    log_failure_to_db(task_id, dag_id, execution_date, error_message, No_of_retries)

def send_success_alert(context):
    print('Task succeeded sending a notification')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    success_message = f'Task {task_id} in DAG {dag_id} succeeded on {execution_date}.'
    log_success_to_db(task_id, dag_id, execution_date, success_message)

# this is where we are sending the data to success table
def log_success_to_db(task_id, dag_id, execution_date, success_message):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_dag_success (task_id, dag_id, execution_date, success_message)
    VALUES (%s, %s, %s, %s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, success_message))


# this is where we are sending the data to fail table
def log_failure_to_db(task_id, dag_id, execution_date, error_message,No_of_retries):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_dag_fail (task_id, dag_id, execution_date, error_message,retry_count)
    VALUES (%s, %s, %s, %s,%s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, error_message,No_of_retries))

# this is where we are extracting the data and comparing the row count of source and destination
def print_data(**kwargs):
    sql_query1 = ''' 
        select count(*) as dest_row_count
        from opd_count                            
        where TRUNC(date)=Trunc(sysdate)
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

    dest_row_count = dest_hook_dest.get_records(sql_query1)[0][0]
    sour_row_count = sour_hook_dest.get_records(sql_query2)[0][0]

    logging.info(f"Destination row count: {dest_row_count}")
    logging.info(f"Source row count: {sour_row_count}")

    # Get execution date from kwargs and convert it to the desired time zone
    execution_date = kwargs['execution_date']
    execution_date_kolkata = execution_date.astimezone(time_zone)
    
    # Convert to string in ISO format
    execution_date_str = execution_date_kolkata.isoformat()

    # Insert row counts into air_row_count table
    insert_sql = '''
    INSERT INTO air_row_count (execution_date, Destination_row, Source_row)
    VALUES (%s, %s, %s);
    '''
    dest_hook_dest.run(insert_sql,parameters=(execution_date_str, dest_row_count, sour_row_count))

    # Compare row counts and trigger an alert if there's a mismatch
    if dest_row_count != sour_row_count:
        logging.error('Row count mismatch detected')
        send_alert(kwargs, dest_row_count, sour_row_count)
    else:
        logging.info('The row counts match')

    return dest_row_count, sour_row_count

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

    # creating the required table at destination
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
# NOTE: copy command is not going to work at the UAT and PROD thats y we have function
    transfer_data = PostgresOperator(
        task_id='transfer_data',
        postgres_conn_id='postgres',
        sql='''
            COPY (SELECT trunc(gdt_entry_date) as Date,
                count(hrgnum_puk) as OPD_count
                FROM hrgt_episode_dtl
                WHERE gnum_isvalid = 1
                AND gnum_hospital_code = 22914
                --and trunc(gdt_entry_date) = trunc(sysdate)
                GROUP BY  trunc(gdt_entry_date))
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

    # this is the row-compare task
    compare_count = PythonOperator(
        task_id='compare_row',
        provide_context=True,
        python_callable=print_data
    )

    create_table >> transfer_data >> load_data >> compare_count 
