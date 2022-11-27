from datetime import datetime

from airflow.models import DAG
from pandas import DataFrame 

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

S3_FILE_PATH ="s3://gaurav-airflowpipeline/orders_data_header.csv"
S3_CONN_ID="aws_default"
SNOWFLAKE_CONN_ID="snowflake_default"
SNOWFLAKE_ORDERS="order_table"
SNOWFLAKE_FILTERED_ORDERS ="filtered_table"
SNOWFLAKE_JOINED="joined_table"
SNOWFLAKE_CUSTOMERS="customers_table"
SNOWFLAKE_REPORTING="reporting_table"

with DAG(dag_id='astro_orders', start_date=datetime(2020,1,1), schedule='@daily', catchup=False):
    orders_data=aql.load_file(
        input_file=File(
            path=S3_FILE_PATH+"/orders_data_header.csv" ,conn_id=S3_CONN_ID
        ),
        output_table=Table(conn_id=SNOWFLAKE_CONN_ID)
    )

    customers_table=Table(
        name=SNOWFLAKE_CUSTOMERS,
        conn_id=SNOWFLAKE_CONN_ID,
    )