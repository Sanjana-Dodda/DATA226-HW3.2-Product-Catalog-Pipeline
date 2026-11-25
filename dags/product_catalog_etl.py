from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

CSV_PATH = "/Users/sanjanadodda/Downloads/DATA226_HW3_2_Project_Package/data/products.csv"
DBT_DIR = "/Users/sanjanadodda/Downloads/DATA226_HW3_2_Project_Package/dbt"

def validate_source():
    import csv
    with open(CSV_PATH, newline="") as f:
        rows=list(csv.DictReader(f))
    if not rows: raise ValueError("Source file is empty")
    required={"product_id","name","category","price","updated_at"}
    if not required.issubset(rows[0]): raise ValueError("Required source columns missing")

def load_snowflake():
    hook=SnowflakeHook(snowflake_conn_id="snowflake_conn")
    hook.run("TRUNCATE TABLE PRODUCT_CATALOG.STAGING.RAW_PRODUCTS")
    conn=hook.get_conn(); cur=conn.cursor()
    import csv
    try:
        with open(CSV_PATH,newline="") as f:
            for r in csv.DictReader(f):
                cur.execute("INSERT INTO PRODUCT_CATALOG.STAGING.RAW_PRODUCTS VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (r['product_id'],r['name'],r['category'],r['price'],r['description'],r['rating'],r['created_at'],r['updated_at']))
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally: cur.close(); conn.close()

def on_failure(context):
    print(f"FAILED task={context['task_instance'].task_id}; exception={context.get('exception')}")

default_args={"owner":"data226","retries":2,"retry_delay":timedelta(minutes=1),"on_failure_callback":on_failure}
with DAG("product_catalog_etl", start_date=datetime(2026,11,1), schedule="@daily", catchup=False, default_args=default_args, tags=["DATA226","dbt","snowflake"]) as dag:
    extract=PythonOperator(task_id="extract_validate_source",python_callable=validate_source)
    load=PythonOperator(task_id="load_snowflake_staging",python_callable=load_snowflake)
    snapshot=BashOperator(task_id="dbt_snapshot", bash_command=f"cd {DBT_DIR} && /opt/miniconda3/envs/data226-dbt/bin/dbt snapshot --profiles-dir ~/.dbt")
    dbt_run=BashOperator(task_id="dbt_run", bash_command=f"cd {DBT_DIR} && /opt/miniconda3/envs/data226-dbt/bin/dbt run --profiles-dir ~/.dbt")
    dbt_test=BashOperator(task_id="dbt_test", bash_command=f"cd {DBT_DIR} && /opt/miniconda3/envs/data226-dbt/bin/dbt test --profiles-dir ~/.dbt")
    extract >> load >> snapshot >> dbt_run >> dbt_test
