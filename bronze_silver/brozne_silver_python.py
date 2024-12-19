# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    tableName = 'mj_lookup'
    snow_df = session.table(tableName)
    src_tables = ['mj_sales','mj_weather_info']
    database_name = "DEMO_DB"
    schema_name = "BRONZE"
    result_list = []
    pk_values_list = []
    
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

            lookup_snow_df = session.table(f'{database_name}.lookup.mj_lookup') \
                    .filter((col("source_table") == src_table) & (col("target_table") == target_table))

            lookup_df = lookup_snow_df.to_pandas()
            cleaned_str = column_set.strip("()")
            column_definitions = cleaned_str.split(",")
            
            # Step 2: Apply the condition to add PRIMARY KEY
            updated_columns = []
            for col_def in column_definitions:
                col_name, col_type = col_def.strip().split(maxsplit=1)
                is_pk = lookup_df[lookup_df['COLUMN_NAME'] == col_name.lower()]\
                                    ['IS_PRIMARY_KEY'].values
                pk_values_list.append(is_pk)
                
                if is_pk == 'TRUE':
                    updated_columns.append(f"{col_name} {col_type} PRIMARY KEY")
                else:
                    updated_columns.append(f"{col_name} {col_type}")
            
            # Step 3: Reformat into the desired format
            result_schema = f"({', '.join(updated_columns)})"
            result_list.append((result_schema))
            
            print(result_schema)
            # lookup_df = lookup_snow_df.to_pandas()
    

    # # Print a sample of the dataframe to standard output.
    # snow_df.show()

    # Return value will appear in the Results tab.
    return pk_values_list