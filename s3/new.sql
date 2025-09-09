-- macros/export_query_to_s3.sql  
{% macro export_query_to_s3(bucket_name, file_prefix='data') %}
  {{ log("Exporting ABC summary to S3", True) }}
  
  {% call statement('export_query', fetch_result=true, auto_begin=true) %}
    COPY INTO 's3://{{bucket_name}}/{{file_prefix}}_{{ run_started_at.strftime('%Y%m%d_%H%M%S') }}.parquet'
    FROM (
      SELECT 
        id,
        SUM(abc) as total_abc
      FROM your_database.your_schema.xyz  -- Replace with your table
      GROUP BY id
    )
    CREDENTIALS = (
      aws_key_id='{{ env_var("AWS_ACCESS_KEY_ID") }}'
      aws_secret_key='{{ env_var("AWS_SECRET_ACCESS_KEY") }}'
    )
    FILE_FORMAT = (
      TYPE = PARQUET
      COMPRESSION = SNAPPY
    )
    OVERWRITE = TRUE
    HEADER = TRUE
  {% endcall %}
  
  {{ log("Export completed", True) }}
{% endmacro %}
