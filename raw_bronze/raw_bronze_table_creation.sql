CREATE OR REPLACE PROCEDURE automate_bronze_layer_py(database_name string, 
raw_schema_name string, 
bronze_schema_name string
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
def run(session, database_name, raw_schema_name, bronze_schema_name):

    exec_list = []
    try:
        schema_name = raw_schema_name  
        bronze_schema_name = bronze_schema_name
        database_name = database_name
    
        # Query to fetch table names and columns from raw layer
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
            GROUP BY table_name
        """
    
        # Execute the query to get table names and column types
        result_set = session.sql(query).collect()
    
        lookup_snow_df = session.table(f'{database_name}.lookup.table_details')
        lookup_df = lookup_snow_df.to_pandas()
    
        # Loop through the result set to create bronze layer tables
        for row in result_set:
            table_name = row['TABLE_NAME']
            if lookup_df[lookup_df['TABLE_NAME'] == table_name]['TABLE_CREATION'].values[0] == 'Y':
                cols_with_types = row['COLUMNS_WITH_TYPES']
                cols_with_types_list = cols_with_types.split(",")
                cols_with_datatypes = {col.strip().split()[0]: col.strip().split()[1] \
                                       for col in cols_with_types_list}
        
                pk_column = lookup_df[lookup_df['TABLE_NAME'] == table_name]['PRIMARY_KEY'].values[0]
            
                for col,item in cols_with_datatypes.items():
                    if col == pk_column:
                        cols_with_datatypes[col] = item + " PRIMARY KEY"
                
                # Convert to the desired format
                formatted_column = ', '.join([f"{col} {dtype}" for col, dtype in                
                 cols_with_datatypes.items()])
        
                # Dynamically generate the SQL to create the bronze table
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {database_name}.{bronze_schema_name}.{table_name}  
                    ({formatted_column})
                    
                """
        
                # Execute the CREATE TABLE statement
                session.sql(create_table_sql).collect()
            exec_list.append(f"Bronze tables created or updated successfully! for {table_name}")

                
    except Exception as e:
        exec_list.append(e)

    return exec_list
$$;



call automate_bronze_layer_py('ADVENTURE_DB','RAW','BRONZE');

select get_ddl('table','adventure_db.bronze.products')