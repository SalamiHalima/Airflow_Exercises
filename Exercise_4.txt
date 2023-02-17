import time 
import json 
from airflow 
import DAG 
from airflow.operators.postgres_operator import PostgresOperator 
from datetime import timedelta

from airflow.utils.dates import days_ago

default_args = { 
    'owner': 'airflow',
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    }
"""Write a DAG which creates an employee table - each row represents a new person and contains info about their name and age
then insert 3 people (with the correct metadata)
finally execute a query which calculates the average age of all employees """

 create_query = """ 
DROP TABLE IF EXISTS public.stock_market_daily;
CREATE TABLE public.stock_market_daily (id INT NOT NULL, ticker varchar(250), price_open FLOAT);
"""

#create a logic that populates the table with some data
insert_data_query = """ 
INSERT TABLE public.employees_table(employee_id, employee_name, employee_age)
values (1, 'Aleema', 24), (2, 'Mercedes', 28), (3, 'Tony', 32)
"""

calculating_averag_age = """ 
DROP TABLE IF EXISTS employees_average_age;
CREATE TABLE IF NOT EXISTS employees_average_age AS 
SELECT employee_name, avg(employee_age)
FROM employees_table
GROUP BY employee_name;
"""

dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1)):

#here you will define the tasks by calling the operator

    create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

    insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

    group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_averag_age, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

    create_table >> insert_data >> group_data