#!/usr/bin/env python3
import re
import sys
from pathlib import Path

def snowflake_to_dbt(sql_content, model_name, source_name="raw"):
    """
    Converts Snowflake SQL to dbt format:
    - Handles schema.table references
    - Converts temp tables to CTEs
    - Adds proper config and documentation
    """
    
    # Add config block
    config = f"""{{{{ config(
    materialized='table',
    schema='{model_name.split('.')[0] if '.' in model_name else 'your_schema'}'
) }}}}

/*
  Generated from Snowflake SQL
  Model name: {model_name}
*/\n\n"""
    
    # Convert schema.table references to source() or ref()
    def replace_table_ref(match):
        schema, table = match.groups()
        # Handle common staging patterns
        if schema.lower().startswith(('stg_', 'stage_', 'staging_')):
            return f"{{{{ ref('{schema.lower()}', '{table.lower()}') }}}}"
        return f"{{{{ source('{source_name}', '{table.lower()}', schema='{schema}') }}}}"
    
    sql_content = re.sub(
        r'(?<!\w)([\w]+)\.([\w]+)(?!\w)',
        replace_table_ref,
        sql_content,
        flags=re.IGNORECASE
    )
    
    # Convert Snowflake-specific functions
    sql_content = sql_content.replace('CURRENT_TIMESTAMP()', "{{ current_timestamp() }}")
    
    # Convert temp tables to CTEs
    temp_table_pattern = r'(create|replace)\s+(temp|temporary)\s+table\s+(\w+)\s+as\s*\(([^;]+)\);?'
    ctes = []
    
    def process_temp_table(match):
        ctes.append(f"{match.group(3)} AS (\n{match.group(4).strip()}\n)")
        return ""
    
    sql_content = re.sub(temp_table_pattern, process_temp_table, sql_content, flags=re.IGNORECASE)
    
    # Build final output
    if ctes:
        sql_content = "WITH " + ",\n\n".join(ctes) + "\n\n" + sql_content.strip()
    
    return config + sql_content

def process_snowflake_file(input_file, output_dir, source_name="raw"):
    """Process a Snowflake SQL file"""
    sql_content = Path(input_file).read_text()
    model_name = Path(input_file).stem
    dbt_content = snowflake_to_dbt(sql_content, model_name, source_name)
    
    # Determine subdirectory structure
    output_subdir = Path(output_dir)
    if '.' in model_name:
        parts = model_name.split('.')
        output_subdir = output_subdir / parts[0]
        model_name = parts[-1]
    
    output_subdir.mkdir(parents=True, exist_ok=True)
    output_path = output_subdir / f"{model_name}.sql"
    output_path.write_text(dbt_content)
    print(f"Converted {input_file} → {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: snowflake_to_dbt.py <input.sql> <output_dir> [source_name]")
        print("Example: snowflake_to_dbt.py query.sql models/ raw")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    source_name = sys.argv[3] if len(sys.argv) > 3 else "raw"
    
    process_snowflake_file(input_file, output_dir, source_name)
