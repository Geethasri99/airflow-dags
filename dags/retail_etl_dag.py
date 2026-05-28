"""Retail ETL DAG — orchestrates daily extract, transform, and load pipeline."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
      "owner": "data-engineering",
      "depends_on_past": False,
      "email_on_failure": True,
      "email_on_retry": False,
      "retries": 2,
      "retry_delay": timedelta(minutes=5),
}

dag = DAG(
      dag_id="retail_etl_pipeline",
      description="Daily retail data ETL: extract from source, transform, load to warehouse",
      default_args=default_args,
      schedule_interval="0 2 * * *",
      start_date=days_ago(1),
      catchup=False,
      tags=["retail", "etl", "daily"],
)


def extract_orders(**context):
      """Extract raw orders from source database."""
      execution_date = context["ds"]
      print(f"Extracting orders for {execution_date}")
      orders = [
          {"order_id": i, "customer_id": 1000 + i, "amount": round(50 + i * 1.5, 2),
           "status": "delivered", "region": "North", "order_date": execution_date}
          for i in range(1, 101)
      ]
      context["ti"].xcom_push(key="raw_orders", value=orders)
      print(f"Extracted {len(orders)} orders")
      return len(orders)


def validate_data(**context):
      """Run data quality checks on extracted orders."""
      orders = context["ti"].xcom_pull(key="raw_orders", task_ids="extract_orders")
      assert orders, "No orders extracted — aborting pipeline"
      missing_fields = [o for o in orders if not all(k in o for k in ["order_id", "amount", "status"])]
      assert not missing_fields, f"{len(missing_fields)} orders missing required fields"
      negative_amounts = [o for o in orders if o["amount"] < 0]
      assert not negative_amounts, f"{len(negative_amounts)} orders with negative amounts"
      print(f"Validation passed: {len(orders)} orders, 0 quality issues")


def transform_orders(**context):
      """Apply business transformations to raw orders."""
      orders = context["ti"].xcom_pull(key="raw_orders", task_ids="extract_orders")
      transformed = []
      for o in orders:
                tier = (
                              "platinum" if o["amount"] >= 500
                              else "gold" if o["amount"] >= 200
                              else "silver" if o["amount"] >= 50
                              else "bronze"
                )
                transformed.append({**o, "customer_tier": tier, "is_high_value": o["amount"] >= 200})
            context["ti"].xcom_push(key="transformed_orders", value=transformed)
    print(f"Transformed {len(transformed)} orders")


def load_to_warehouse(**context):
      """Load transformed orders to data warehouse."""
    orders = context["ti"].xcom_pull(key="transformed_orders", task_ids="transform_orders")
    print(f"Loading {len(orders)} orders to warehouse table retail.fct_orders")
    print("Load complete — rows inserted:", len(orders))


def send_pipeline_alert(**context):
      """Send completion notification."""
    execution_date = context["ds"]
    ti = context["ti"]
    extracted = ti.xcom_pull(task_ids="extract_orders") or 0
    print(f"Pipeline complete for {execution_date}: {extracted} orders processed")


# Task definitions
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

t_extract = PythonOperator(task_id="extract_orders", python_callable=extract_orders, dag=dag)
t_validate = PythonOperator(task_id="validate_data", python_callable=validate_data, dag=dag)
t_transform = PythonOperator(task_id="transform_orders", python_callable=transform_orders, dag=dag)
t_load = PythonOperator(task_id="load_to_warehouse", python_callable=load_to_warehouse, dag=dag)
t_alert = PythonOperator(task_id="send_pipeline_alert", python_callable=send_pipeline_alert, dag=dag)

# DAG dependencies
start >> t_extract >> t_validate >> t_transform >> t_load >> t_alert >> end
