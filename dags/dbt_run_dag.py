"""dbt Run DAG — triggers dbt model runs via BashOperator on a daily schedule."""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = "/opt/airflow/dbt/retail_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"

default_args = {
      "owner": "data-engineering",
      "depends_on_past": False,
      "email_on_failure": True,
      "email_on_retry": False,
      "retries": 1,
      "retry_delay": timedelta(minutes=10),
}

dag = DAG(
      dag_id="dbt_retail_models",
      description="Daily dbt run: staging models then mart models with tests",
      default_args=default_args,
      schedule_interval="0 3 * * *",
      start_date=days_ago(1),
      catchup=False,
      tags=["dbt", "retail", "daily"],
)


dbt_cmd = f"dbt --no-write-json --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

debug = BashOperator(
      task_id="dbt_debug",
      bash_command=f"{dbt_cmd} debug",
      dag=dag,
)

run_staging = BashOperator(
      task_id="dbt_run_staging",
      bash_command=f"{dbt_cmd} run --select staging",
      dag=dag,
)

test_staging = BashOperator(
      task_id="dbt_test_staging",
      bash_command=f"{dbt_cmd} test --select staging",
      dag=dag,
)

run_marts = BashOperator(
      task_id="dbt_run_marts",
      bash_command=f"{dbt_cmd} run --select marts",
      dag=dag,
)

test_marts = BashOperator(
      task_id="dbt_test_marts",
      bash_command=f"{dbt_cmd} test --select marts",
      dag=dag,
)

generate_docs = BashOperator(
      task_id="dbt_generate_docs",
      bash_command=f"{dbt_cmd} docs generate",
      dag=dag,
)

def log_completion(**context):
      print(f"dbt pipeline complete for {context['ds']}")
      print("Models run: staging/* marts/*")

notify = PythonOperator(task_id="notify_completion", python_callable=log_completion, dag=dag)

start >> debug >> run_staging >> test_staging >> run_marts >> test_marts >> generate_docs >> notify >> end
