from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
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

def send_alert(context,error_msg, dest_row_count=None, sour_row_count=None):
    print('Task failed sending an Email')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = str(context.get('execution_date'))
    exception = context.get('exception')
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']
    # If the source and destination row count is not same then else will run
    if dest_row_count is None or sour_row_count is None:
        error_message = str(exception)
    else:
        error_message = error_msg

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
    log_failure_to_db(task_id, dag_id, execution_date, error_message,No_of_retries)


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
    INSERT INTO air_abdm_success (task_id, dag_id, execution_date, success_message)
    VALUES (%s, %s, %s, %s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, success_message))


def log_failure_to_db(task_id, dag_id, execution_date, error_message,No_of_retries):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_abdm_fail (task_id, dag_id, execution_date, error_message,retry_count)
    VALUES (%s, %s, %s, %s,%s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, error_message,No_of_retries))


def export_data_staging(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='abdm_uat_connection',schema = 'abdm')
    destination_hook = PostgresHook(postgres_conn_id='destination_conn_id', schema='Airflow_destination')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = ''' WITH HealthRecord_count AS (
                SELECT  count(*) AS Healthrecord_count,
                        str_hospital_code AS Hospital_code,
                        dt_entry_date::DATE AS DATE
                FROM abdm_connector.tbl_log_care_context
                WHERE   str_api_key = 'on-add_care_context'
                        AND str_status = '1'
                        AND str_response_body::json -> 'acknowledgement' ->> 'status' = 'SUCCESS'
                        AND num_isvalid = 1
                        AND str_hospital_code IN ('37913','98926')
                        AND dt_entry_date >= CURRENT_DATE - 1
                        AND dt_entry_date < CURRENT_DATE
                        GROUP BY str_hospital_code, dt_entry_date::DATE
                ),
                token_count AS (
                SELECT  dt_entry_date::DATE AS DATE,
                        str_hospital_code AS hospital_code,
                        COUNT(*) AS Token_count
                FROM abdm_connector.tbl_log_api_shareprofile_hip
                WHERE dt_entry_date >= CURRENT_DATE - 1
                AND dt_entry_date < CURRENT_DATE
                AND str_api_key = 'ON_PROFILE_SHARE_HIP'
                AND str_status = '1'
                AND str_request_body NOT LIKE '%"tokenNumber":"00%'
                AND str_hospital_code IN ('37913','98926')
                GROUP BY str_hospital_code, dt_entry_date::DATE
                )
                SELECT H.Healthrecord_count,
                        H.DATE,
                        T.Token_count,
                        COUNT(*) OVER() AS source_row_count	
                FROM HealthRecord_count H
                JOIN token_count T ON H.DATE = T.DATE
                AND H.Hospital_code = T.hospital_code;
            '''
    
    cursor.execute(query)
    rows = cursor.fetchall()
    print('printing the rows:',rows)
    source_row_count = len(rows)

    # Get execution date from kwargs and convert it to the desired time zone
    execution_date = kwargs['execution_date']
    execution_date_kolkata = execution_date.astimezone(time_zone)
    # Convert to string in ISO format
    execution_date_str = execution_date_kolkata.isoformat()

    # Now switch to the destination connection to insert the data
    dest_conn = destination_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    insert_sql = '''
    INSERT INTO air_abdm_row_count (execution_date,source_row) VALUES (%s,%s);
    '''
    # Execute the insert query on the destination DB
    dest_cursor.execute(insert_sql, (execution_date_str, source_row_count))
    dest_conn.commit()
    # Close both cursors and connections
    cursor.close()
    conn.close()

    dest_cursor.close()
    dest_conn.close()

    # writing the data and after the transfer the data is deleted
    with open('/tmp/staging_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Date','facility_id','Token_count','Healthrecord_count'])
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
                'INSERT INTO air_abdm_token_count (Date, facility_id,Token_count,Healthrecord_count) VALUES (%s, %s, %s, %s)',
                row
            )
    
    conn.commit()
    cursor.close()
    conn.close()

def print_data(**kwargs):
    sql_query1 = ''' 
        select count(*) as air_abdm_row_count
        from air_abdm_token_count
		where trunc(date_time) = trunc(sysdate)-1
    '''

    sql_query2 = '''select source_row
                from air_abdm_row_count
                where trunc(execution_date) = trunc(sysdate)-1'''
    
    dest_hook_dest = PostgresHook(postgres_conn_id='destination_conn_id', schema='Airflow_destination')
    source_row_count = PostgresHook(postgres_conn_id='destination_conn_id', schema='Airflow_destination')

    dest_row_count = dest_hook_dest.get_records(sql_query1)[0]
    source_row_count = source_row_count.get_records(sql_query2)[0]

    # Insert row counts into air_row_count table
    insert_sql = '''
    INSERT INTO air_abdm_row_count (Destination_row) VALUES (%s);
    '''
    dest_hook_dest.run(insert_sql, parameters=(dest_row_count))


    if source_row_count!=dest_row_count:
        error_message = 'Source and destination row are not same'
        send_alert(kwargs,error_message,dest_hook_dest,source_row_count)
    else:
        logging.info('The Rows are same')

    return dest_row_count,source_row_count



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
        dag_id="ABDM_data_transfer",
        default_args=default_args,
        description="Transferring the data from ABDM UAT to development",
        schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
        # 5 - Mins , 11-Hours ,* - any day of week ,*- any month,*-any day of week 
        catchup=False
    ) as dag:

    # Extracting the data from source and loading it into the staging area
    export_task = PythonOperator(
        task_id='export_data_to_csv',
        python_callable=export_data_staging,
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
    export_task >> load_data >> compare_count 