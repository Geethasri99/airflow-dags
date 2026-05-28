# airflow-dags

Apache Airflow DAG orchestration for retail data pipelines — daily ETL workflows, dbt model runs, data quality checks, and completion alerts.

## Architecture

```
[Source DB] --> extract_orders --> validate_data --> transform_orders --> load_to_warehouse --> alert
                                                                         |
[dbt models] --> dbt_debug --> run_staging --> test_staging --> run_marts --> test_marts --> docs
```

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `retail_etl_pipeline` | Daily 2am | Extract, validate, transform, and load retail orders |
| `dbt_retail_models` | Daily 3am | Run and test dbt staging and mart models |

## Features

- Retry logic with configurable backoff on all tasks
- - XCom-based data passing between tasks
  - - Data quality assertions (null checks, schema validation, range checks)
    - - dbt staging + marts separation with tests at each layer
      - - BashOperator dbt integration with profiles dir support
        - - EmptyOperator bookend tasks for clear DAG structure
         
          - ## Quick Start
         
          - ```bash
            pip install -r requirements.txt
            export AIRFLOW_HOME=~/airflow
            airflow db init
            airflow dags list
            airflow dags trigger retail_etl_pipeline
            ```

            ## Tech Stack

            - Apache Airflow 2.8+
            - - dbt Core 1.7+ with Snowflake adapter
              - - Python 3.10+
                - - PostgreSQL (metadata DB)
                  - - Amazon S3 (data lake)
