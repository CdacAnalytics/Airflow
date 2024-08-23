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
import csv
from datetime import datetime, timedelta
import pendulum
from airflow.utils.email import send_email
from email.mime.text import MIMEText
import smtplib
import psycopg2



# setting the time as indian standard time. We have to set this if we want to schedule a pipeline 
time_zone = pendulum.timezone("Asia/Kolkata")

def send_alert(context, dest_row_count=None, sour_row_count=None):
    print('Task failed sending an Email')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = str(context.get('execution_date'))
    exception = context.get('exception')
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']
    hospital_name = 'Aiims_kalyani'
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
    if dest_row_count is not None and sour_row_count is not None:
        body += f"""
        <br>Source Row Count: {sour_row_count}</br>
        <br>Destination Row Count: {dest_row_count}</br>
        """
    
    # Using Airflow's send_email function for consistency and better integration
    send_email(
        to='gauravnagraleofficial@gmail.com',
        subject=subject,
        html_content=body
    )

    log_failure_to_db(task_id, dag_id, execution_date, error_message,No_of_retries,hospital_name)

def send_success_alert(context):
    print('Task succeeded sending a notification')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    hospital_name = 'Aiims_Kalyani'
    success_message = f'Task {task_id} in DAG {dag_id} succeeded on {execution_date}.'
    log_success_to_db(task_id, dag_id, execution_date, success_message,hospital_name)


def log_success_to_db(task_id, dag_id, execution_date, success_message,hospital_name):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_dag_success (task_id, dag_id, execution_date, success_message,hospital_name)
    VALUES (%s, %s, %s, %s,%s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, success_message,hospital_name))


def log_failure_to_db(task_id, dag_id, execution_date, error_message,No_of_retries,hospital_name):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_dag_fail (task_id, dag_id, execution_date, error_message,retry_count,hospital_name)
    VALUES (%s, %s, %s, %s,%s,%s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, error_message,No_of_retries,hospital_name))

def print_data(**kwargs):
    sql_query1 = ''' 
        select count(*) as dest_row_count
        from opd_count
        where TRUNC(date) = TRUNC(SYSDATE)-1;
    '''

    sql_query2 = '''       
        with data as (
            select count(hrgnum_puk) 
            from hrgt_episode_dtl
            where gnum_isvalid = 1
            and gnum_hospital_code = 22914
            and TRUNC(gdt_entry_date) = TRUNC(SYSDATE)-1
        )
        select count(*) as Source_row_count from data;
    '''
    
    dest_hook_dest = PostgresHook(postgres_conn_id='destination_conn_id', schema='Airflow_destination')
    sour_hook_dest = PostgresHook(postgres_conn_id='postgres', schema='aiimsnew')

    dest_row_count = dest_hook_dest.get_records(sql_query1)[0]
    sour_row_count = sour_hook_dest.get_records(sql_query2)[0]

    logging.info(f"Query result: {dest_row_count}")
    logging.info(f"Query result: {sour_row_count}")

    
    # Get execution date from kwargs and convert it to the desired time zone
    execution_date = kwargs['execution_date']
    execution_date_kolkata = execution_date.astimezone(time_zone)
    
    # Convert to string in ISO format
    execution_date_str = execution_date_kolkata.isoformat()

    # Insert row counts into air_row_count table
    insert_sql = '''
    INSERT INTO air_row_count (execution_date,Destination_row, Source_row)
    VALUES (%s, %s, %s);
    '''
    dest_hook_dest.run(insert_sql, parameters=(execution_date_str, sour_row_count, dest_row_count))

    if sour_row_count!=dest_row_count:
        send_alert(kwargs)
    else:
        logging.info('The Rows are same')

    return dest_row_count,sour_row_count


# transferring the data into a staging area
    # with clause specifies that the format for the output should be saved as CSV 
    # and this data is stored in container running in docker
    # the data will be lost once the container is stopped or removed
def export_data_to_csv():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """
        SELECT trunc(E.gdt_entry_date) as Date,
                count(E.hrgnum_puk) as OPD_count,
		        gstr_hosp_short_name as Hospital_name
        FROM hrgt_episode_dtl E, gblt_hospital_mst H
        WHERE H.gnum_hospital_code = E.gnum_hospital_code
        and E.gnum_isvalid = 1
        and H.gnum_isvalid = 1
        and E.gnum_hospital_code = 22914
        and trunc(E.gdt_entry_date) = trunc(sysdate)-10
        GROUP BY trunc(E.gdt_entry_date),Hospital_name;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    # writing the data and after the transfer the data is deleted
    with open('/tmp/staging_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'OPD_count'])
        writer.writerows(rows)
    
    with open('/tmp/staging_data.csv', 'r') as file:
        read = csv.reader(file)
        print('printing the rows:')
        for rows in read:
            print(rows)

def load_csv_to_postgres():
    conn = psycopg2.connect(
        dbname="Airflow_destination",
        user="aiimsnew",
        password="aiimsnew",
        host="10.226.80.35",
        port="5445"
    )
    cursor = conn.cursor()
    
    with open('/tmp/staging_data.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        for row in reader:
            cursor.execute(
                "INSERT INTO OPD_count (Date, OPD_count,Hospital_name) VALUES (%s, %s, %s)",
                row
            )
    
    conn.commit()
    cursor.close()
    conn.close()
    

# Default arguments for the DAG
default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert,
}

# Define the DAG
with DAG(
        dag_id="OPD_count",
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

    # Extracting the data from source and loading it into the staging area
    export_task = PythonOperator(
        task_id='export_data_to_csv',
        python_callable=export_data_to_csv,
        dag=dag
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_csv_to_postgres,
        dag=dag
    )

    compare_count = PythonOperator(
        task_id='compare_row',
        provide_context=True,
        python_callable=print_data,
        on_success_callback=send_success_alert
    )
    create_table >> export_task >> load_data >> compare_count 
