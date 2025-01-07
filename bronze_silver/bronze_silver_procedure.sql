CREATE OR REPLACE PROCEDURE demo_db.silver.CREATE_SILVER_TABLES(
    LOOKUP_TABLE STRING,
    SRC_TABLES ARRAY,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    SILVER_SCHEMA_NAME STRING
)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = (
    'snowflake-snowpark-python', 
    'pandas'
)
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def main(session: Session,
         lookup_table: str,
         src_tables: list,
         database_name: str,
         schema_name: str,
         silver_schema_name: str):
    
    exec_list = []
    try: 
        # 1) Convert your parameters
        lookup_table_name = lookup_table
        
        # 2) Read your lookup table into a Snowpark DataFrame
        snow_df = session.table(lookup_table_name)
        
        # 3) Collect the target DataFrames for each source table
        target_dfs_list = []
        for src_table in src_tables:
            target_dfs_list.append(snow_df.filter(col("source_table") == src_table))
            
        # 4) Iterate over each DataFrame (i.e., each source table)
        for table_df in target_dfs_list:
            
            # Distinct target tables for this source
            dim_tables_df = table_df.select('target_table').distinct()
            
            for dim_table_df in dim_tables_df.collect():
                target_table = dim_table_df[0]
                
                # Filter out rows relevant to this one target table
                dim_table = table_df.filter(col("target_table") == target_table)
                
                # Extract the single source_table value
                source_table = table_df.select('source_table').distinct().collect()[0][0]
                
                # Collect the columns from the lookup
                col_names = tuple([c[0].strip().upper() 
                                   for c in dim_table.select('column_name').collect()])
                
                # Build a query to fetch column metadata from information_schema
                query = f"""
                  SELECT table_name, 
                         LISTAGG(
                           CASE 
                             WHEN data_type = 'TEXT' THEN column_name || ' VARCHAR' 
                             ELSE column_name || ' ' || data_type
                           END, 
                           ', '
                         ) AS columns_with_types
                  FROM {database_name}.information_schema.columns
                  WHERE table_schema = '{schema_name}'
                    AND table_name = '{source_table.upper()}'
                    AND column_name IN {col_names}
                  GROUP BY table_name
                """
                
                result_set = session.sql(query).collect()
                if not result_set:
                    exec_list.append(f"No matching columns found for {source_table} in information_schema.")
                    continue
                
                column_set = f"({result_set[0][1]})"
                
                # Retrieve the lookup info for PK/FK transformations
                lookup_snow_df = session.table(f"{database_name}.look_up.mj_lookup") \
                                         .filter((col("source_table") == source_table) &
                                                 (col("target_table") == target_table))
                
                lookup_df = lookup_snow_df.to_pandas()
                
                # Clean parentheses
                cleaned_str = column_set.strip("()")
                column_definitions = cleaned_str.split(",")
                
                # Step 2: Apply PK/FK logic
                updated_columns = []
                for col_def in column_definitions:
                    col_def = col_def.strip()
                    # Split into column_name, column_type
                    parts = col_def.split(maxsplit=1)
                    if len(parts) != 2:
                        # Skip if something is off in the definition
                        continue
                    
                    col_name, col_type = parts[0], parts[1]
                    
                    # Match row in lookup
                    match_pk = lookup_df[lookup_df['COLUMN_NAME'] == col_name.lower()]['IS_PRIMARY_KEY'].values
                    match_fk = lookup_df[lookup_df['COLUMN_NAME'] == col_name.lower()]['TRANSFORMATION'].values
                    
                    # Defaults
                    is_pk = (len(match_pk) > 0 and match_pk[0] == 'TRUE')
                    is_fk = (len(match_fk) > 0 and match_fk[0] is not None)
                    
                    # Combine PK/FK logic
                    if is_pk and is_fk:
                        updated_columns.append(
                            f"{col_name} {col_type} PRIMARY KEY REFERENCES {schema_name}.{source_table}({col_name})"
                        )
                    elif is_pk:
                        updated_columns.append(f"{col_name} {col_type} PRIMARY KEY")
                    elif is_fk:
                        updated_columns.append(
                            f"{col_name} {col_type} REFERENCES {schema_name}.{source_table}({col_name})"
                        )
                    else:
                        updated_columns.append(f"{col_name} {col_type}")
                
                # Step 3: Reformat into the desired column definition
                formatted_columns = f"({', '.join(updated_columns)})"
                
                # Create the table in the Silver schema if it doesn't exist
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {database_name}.{silver_schema_name}.{target_table}  
                    {formatted_columns}
                """
                
                session.sql(create_table_sql).collect()
                exec_list.append(f"Created or updated table: {target_table} in {silver_schema_name}")
                
    except Exception as e:
        exec_list.append(str(e))
    
    # Return the execution log or messages as an array
    return exec_list
$$;

CALL demo_db.silver.CREATE_SILVER_TABLES(
    'demo_db.look_up.mj_lookup', 
    ARRAY_CONSTRUCT('mj_sales','mj_weather_info'),
    'DEMO_DB',
    'BRONZE',
    'SILVER'
);

-- drop table demo_db.silver.dim_customer_mj;
-- drop table demo_db.silver.dim_product_mj;
-- drop table demo_db.silver.dim_weather_details_mj;
-- drop table demo_db.silver.dim_weather_severity_mj;
-- drop table demo_db.silver.fct_transactions_mj;
-- drop table demo_db.silver.fct_weather_mj;

-- select get_ddl('table','DEMO_DB.SILVER.DIM_WEATHER_DETAILS_MJ');
