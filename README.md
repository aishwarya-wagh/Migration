# Comprehensive Guide to Migrating ETL to dbt

## Introduction to dbt (Data Build Tool)

dbt (data build tool) is a transformation workflow that enables data analysts and engineers to transform data in their warehouses more effectively. It allows you to:
- Write modular data transformations as SQL SELECT statements
- Manage dependencies between transformations
- Test and document your data
- Version control your data transformation logic

### Key Concepts

1. **Models**: SQL files that define your transformations (equivalent to tables/views in your warehouse)
2. **Sources**: Definitions of your raw data tables
3. **Tests**: Data quality checks you can run on your models
4. **Documentation**: Descriptions of your models, columns, and business logic
5. **Macros**: Reusable SQL snippets (like functions in programming)

## Setting Up dbt

### Prerequisites
- A data warehouse (Snowflake, BigQuery, Redshift, etc.)
- Python 3.7+
- pip (Python package manager)

### Installation
```bash
pip install dbt-core
# Plus the adapter for your warehouse, e.g.:
pip install dbt-snowflake
```

### Initialize a dbt Project
```bash
dbt init my_dbt_project
```

## Project Structure

A typical dbt project looks like:
```
my_dbt_project/
├── dbt_project.yml      # Project configuration
├── models/              # Your SQL models
│   ├── staging/         # Staging models (raw data transformations)
│   ├── marts/           # Business-facing models
│   ├── schema.yml       # Documentation and tests
├── seeds/               # CSV files to load as tables
├── snapshots/           # Slowly changing dimension logic
├── macros/              # Reusable SQL snippets
├── tests/               # Custom data tests
└── analyses/            # Exploratory SQL files
```

## Migrating ETL to dbt: Step-by-Step

### 1. Analyze Your Existing ETL

Before migrating:
- Document all current ETL jobs
- Identify dependencies between jobs
- Note any business logic in transformations
- Document data quality checks

### 2. Set Up Source Definitions

In `models/staging/sources.yml`:
```yaml
version: 2

sources:
  - name: production_database
    database: production_db
    schema: raw
    tables:
      - name: orders
        description: "Raw orders data from production database"
        loaded_at_field: created_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
```

### 3. Create Staging Models

First layer of transformation - cleaning and standardizing raw data.

Example: `models/staging/stg_orders.sql`
```sql
{{
  config(
    materialized='view',
    alias='stg_orders'
  )
}}

SELECT
    id AS order_id,
    customer_id,
    order_date,
    amount,
    status,
    created_at AS loaded_at
FROM {{ source('production_database', 'orders') }}
WHERE status != 'test'
```

### 4. Build Intermediate Models

Transformations that join staging models or add business logic.

Example: `models/marts/intermediate/int_customer_orders.sql`
```sql
{{
  config(
    materialized='table',
    alias='int_customer_orders'
  )
}}

WITH order_totals AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(amount) AS lifetime_value
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    o.order_count,
    o.lifetime_value,
    CASE
        WHEN o.lifetime_value > 1000 THEN 'VIP'
        WHEN o.lifetime_value > 500 THEN 'Premium'
        ELSE 'Standard'
    END AS customer_segment
FROM {{ ref('stg_customers') }} c
LEFT JOIN order_totals o ON c.customer_id = o.customer_id
```

### 5. Create Final Mart Models

Business-facing models ready for analytics.

Example: `models/marts/core/dim_customers.sql`
```sql
{{
  config(
    materialized='table',
    alias='dim_customers',
    unique_key='customer_id'
  )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    order_count,
    lifetime_value,
    customer_segment,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('int_customer_orders') }}
```

### 6. Add Tests and Documentation

In `models/marts/core/schema.yml`:
```yaml
version: 2

models:
  - name: dim_customers
    description: "Customer dimension table with segmentation"
    columns:
      - name: customer_id
        description: "Primary key for customer"
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: email
```

### 7. Implement Incremental Models

For large tables, use incremental models to process only new data.

Example: `models/staging/stg_events.sql`
```sql
{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
  )
}}

SELECT
    event_id,
    user_id,
    event_type,
    event_time,
    event_data
FROM {{ source('production_database', 'events') }}

{% if is_incremental() %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
```

### 8. Create Macros for Reusable Logic

In `macros/date_utils.sql`:
```sql
{% macro date_trunc(column_name, date_part='day') -%}
  {{ return(adapter.dispatch('date_trunc', 'dbt_utils')(column_name, date_part)) }}
{%- endmacro %}

{% macro dollars_to_cents(column_name) -%}
  ({{ column_name }} * 100)::integer
{%- endmacro %}
```

### 9. Set Up Snapshots for Slowly Changing Dimensions

In `snapshots/scd_customers.sql`:
```sql
{% snapshot scd_customers %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
  )
}}

SELECT * FROM {{ source('production_database', 'customers') }}

{% endsnapshot %}
```

### 10. Implement Custom Tests

In `tests/assert_positive_value.sql`:
```sql
{% test assert_positive_value(model, column_name) %}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
```

## Running dbt

### Common Commands

```bash
# Run all models
dbt run

# Run specific models
dbt run --models stg_orders+

# Test all models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run only models that changed since last run
dbt run --select state:modified
```

### Scheduling dbt Jobs

Options include:
- Airflow
- dbt Cloud
- Prefect/Dagster
- Cron jobs

Example Airflow DAG:
```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_daily_run',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt/project && dbt run',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/dbt/project && dbt test',
    dag=dag,
)

dbt_run >> dbt_test
```

## Advanced dbt Features

### 1. Hooks

Run operations before or after models:

```yaml
# dbt_project.yml
on-run-start:
  - "{{ log('Starting dbt run!', info=True) }}"
  - "CREATE SCHEMA IF NOT EXISTS {{ target.schema }}"

on-run-end:
  - "GRANT SELECT ON ALL TABLES IN SCHEMA {{ target.schema }} TO reporting_role"
```

### 2. Packages

Reuse community-contributed code:

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.8.0
  - package: calogica/dbt_expectations
    version: 0.5.0
```

Install with:
```bash
dbt deps
```

### 3. Variables

Pass dynamic values to your project:

```sql
-- In models
SELECT * FROM {{ var('target_database') }}.schema.table
```

```yaml
# dbt_project.yml
vars:
  target_database: production
```

Override from command line:
```bash
dbt run --vars 'target_database: development'
```

### 4. Tags

Organize and select models:

```sql
{{ config(tags=['finance', 'daily']) }}
```

Run only tagged models:
```bash
dbt run --select tag:daily
```

## Migration Best Practices

1. **Start Small**: Begin with a single ETL pipeline
2. **Maintain Parallel Runs**: Run both old and new pipelines during migration
3. **Validate Outputs**: Compare results between old and new systems
4. **Document Everything**: Use dbt's documentation features
5. **Implement CI/CD**: Set up testing and deployment pipelines
6. **Monitor Performance**: Track run times and resource usage
7. **Train Your Team**: Ensure everyone understands dbt concepts

## Common ETL to dbt Patterns

### 1. Stored Procedure Migration

Before (SQL Stored Procedure):
```sql
CREATE PROCEDURE transform_orders()
AS $$
BEGIN
  CREATE TABLE transformed_orders AS
  SELECT id, customer_id, amount FROM raw_orders;
END
$$;
```

After (dbt model `models/transformed_orders.sql`):
```sql
SELECT 
  id, 
  customer_id, 
  amount 
FROM {{ ref('raw_orders') }}
```

### 2. Scheduled Job Migration

Before (Cron job running SQL script):
```bash
0 3 * * * psql -f transform_orders.sql
```

After (dbt command):
```bash
0 3 * * * cd /dbt_project && dbt run --models transformed_orders
```

### 3. Data Validation Migration

Before (SQL assertions):
```sql
-- In validation script
SELECT COUNT(*) FROM orders WHERE amount < 0;
-- If count > 0, send alert
```

After (dbt test):
```yaml
# In schema.yml
columns:
  - name: amount
    tests:
      - not_null
      - accepted_values:
          values: ['> 0']
```

## Performance Optimization

1. **Materialization Strategies**:
   - Tables for frequently queried models
   - Views for lightweight transformations
   - Incremental for large tables

2. **Query Optimization**:
   - Use CTEs for readability and performance
   - Limit columns selected
   - Apply filters early

3. **Parallelism**:
   ```bash
   dbt run --threads 4
   ```

4. **Model Selection**:
   ```bash
   dbt run --exclude tag:heavy
   ```

## Monitoring and Logging

1. **dbt Cloud**:
   - Built-in run history and logging
   - Error tracking
   - Performance metrics

2. **Self-hosted Options**:
   - Log to Snowflake/other warehouse
   - Use Metabase/Tableau for monitoring
   - Integrate with Datadog/other APM tools

Example logging table:
```sql
CREATE TABLE dbt_audit.logs (
    run_id STRING,
    invocation_id STRING,
    model_name STRING,
    status STRING,
    rows_affected INTEGER,
    execution_time FLOAT,
    run_start_at TIMESTAMP,
    run_completed_at TIMESTAMP
);
```

## Conclusion

Migrating from traditional ETL to dbt involves:
1. Understanding your current ETL processes
2. Setting up a structured dbt project
3. Gradually converting transformations to dbt models
4. Implementing testing and documentation
5. Setting up proper scheduling and monitoring

The benefits include:
- Improved collaboration through version control
- Better documentation and data quality
- More maintainable transformation code
- Faster iteration on data models

Remember that migration is an iterative process. Start with a small, high-impact area of your ETL pipeline, learn from that experience, and gradually expand your dbt implementation.
