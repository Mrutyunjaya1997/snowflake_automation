CREATE OR REPLACE PROCEDURE MERGE_RAW_BRONZE_LAYER(database_name string, 
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

    schema_name = raw_schema_name  
    bronze_schema_name = bronze_schema_name
    database_name = database_name
    
    err_list = []

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
            bronze_table_name = table_name.replace('RAW', 'BRONZE')
            columns_with_types = row['COLUMNS_WITH_TYPES']
    
            pk_query = f"SHOW PRIMARY KEYS IN {database_name}.{bronze_schema_name}.{table_name};"
            result = session.sql(pk_query).collect()
    
            # Check if primary key exists, then get the column name
            if result:
                pk_column = result[0]['column_name']
    
                merge_query = f"""
                    MERGE INTO {database_name}.{bronze_schema_name}.{bronze_table_name} AS target
                    USING (
                            SELECT a.*
                            FROM {database_name}.{schema_name}.{table_name} a
                        JOIN (
                            SELECT {pk_column}, MAX(DATE_INSERTED) AS MaxDate
                            FROM {database_name}.{schema_name}.{table_name}
                            GROUP BY {pk_column}
                        ) b ON a.{pk_column} = b.{pk_column} AND a.DATE_INSERTED = b.MaxDate
                    ) AS source
                    ON target.{pk_column} = source.{pk_column}
                    WHEN MATCHED THEN
                        UPDATE SET 
                            {', '.join([f'target.{col.split()[0]} = source.{col.split()[0]}' for col in columns_with_types.split(', ')])}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join([col.split()[0] for col in columns_with_types.split(', ')])})
                        VALUES ({', '.join([f'source.{col.split()[0]}' for col in columns_with_types.split(', ')])});
                """
    
                try:
                    session.sql(merge_query).collect()
                    err_list.append(f"Merge query succeeded for table: {bronze_table_name}")
                except Exception as e:
                    err_list.append(f"Error for table: {bronze_table_name} with error: {e}")
            else:
                err_list.append(f"No primary key found for table: {table_name}")

    return "\n".join(err_list)
$$;


call merge_raw_bronze_layer('ADVENTURE_DB','RAW','BRONZE');
