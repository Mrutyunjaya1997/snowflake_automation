# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import os

def create_snowpark_session():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE")
    }
    return snowpark.Session.builder.configs(connection_parameters).create()

def main(session: snowpark.Session, lookup_table, src_tables, database_name, schema_name, silver_schema_name):
    exec_list = []
    try: 
        # Your code goes here, inside the "main" handler.
        lookup_table_name = lookup_table
        snow_df = session.table(lookup_table_name)
        src_tables = src_tables
        database_name = database_name
        schema_name = schema_name
        silver_schema_name = silver_schema_name
        
        target_dfs_list = []
        for src_table in src_tables:
            target_dfs_list.append(snow_df.filter(col("source_table") == src_table))
            
        for table_df in target_dfs_list:
            dim_tables_df = table_df.select('target_table').distinct()

            for dim_table_df in dim_tables_df.collect():
                dim_table = table_df.filter(col("target_table") == dim_table_df[0])
                target_table = dim_table.select('target_table').distinct().collect()[0][0]
                source_table = table_df.select('source_table').distinct().collect()[0][0]
                col_names = tuple([col[0].strip().upper() for col in dim_table.select('column_name').collect()])
                
                query = f"""
                SELECT table_name, 
                    LISTAGG(
                        CASE 
                            WHEN data_type = 'TEXT' THEN column_name || ' VARCHAR' 
                            ELSE column_name || ' ' || data_type
                        END, 
                        ', ') AS columns_with_types
                FROM {database_name}.information_schema.columns
                WHERE table_schema = '{schema_name}'
                and table_name = '{source_table.upper()}'
                and column_name in {col_names}
                group by table_name
                """
                result_set = session.sql(query).collect()
                column_set = f'({result_set[0][1]})'

                lookup_snow_df = session.table(f'{database_name}.look_up.mj_lookup') \
                        .filter((col("source_table") == source_table) & (col("target_table") == target_table))

                lookup_df = lookup_snow_df.to_pandas()
                cleaned_str = column_set.strip("()")
                column_definitions = cleaned_str.split(",")
                
                # Step 2: Apply the condition to add PRIMARY KEY
                updated_columns = []
                for col_def in column_definitions:
                    col_name, col_type = col_def.strip().split(maxsplit=1)
                    is_pk = lookup_df[lookup_df['COLUMN_NAME'] == col_name.lower()]\
                                        ['IS_PRIMARY_KEY'].values
                    is_fk = lookup_df[lookup_df['COLUMN_NAME'] == col_name.lower()]\
                                        ['TRANSFORMATION'].values
                    
                    if is_pk[0] == 'TRUE' and is_fk[0] is not None:
                        updated_columns.append(f"{col_name} {col_type} PRIMARY KEY \
                                               REFERENCES {schema_name}.{source_table}({col_name})")
                    elif is_pk[0] == 'TRUE':
                        updated_columns.append(f"{col_name} {col_type} PRIMARY KEY")
                    elif is_fk[0] is not None:
                        updated_columns.append(f"{col_name} {col_type} REFERENCES  \
                                               {schema_name}.{source_table}({col_name})")
                    else:
                        updated_columns.append(f"{col_name} {col_type}")

                
                # Step 3: Reformat into the desired format
                formatted_column = f"({', '.join(updated_columns)})"
                print(formatted_column)

                # Dynamically generate the SQL to create the silver table
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {database_name}.{silver_schema_name}.{target_table}  
                    {formatted_column}
                    
                """
                # Execute the CREATE TABLE statement
                session.sql(create_table_sql).collect()
                exec_list.append(f"Silver tables created or updated successfully! \
                                  for {source_table}:{target_table}")
    except Exception as e:
        exec_list.append(e)
            
    # Return value will appear in the Results tab.
    return exec_list


if __name__ == "__main__":
    lookup_table = "mj_lookup_new"
    src_tables = ['mj_weather_info','mj_sales']
    database_name = "DEMO_DB"
    schema_name = "BRONZE"
    silver_schema_name = "SILVER"
    # Create a Snowpark session
    session = create_snowpark_session()
    result = main(session, lookup_table, src_tables, database_name, schema_name, silver_schema_name)
    print(result)