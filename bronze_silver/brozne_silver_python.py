# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    tableName = 'mj_lookup_sales'
    snow_df = session.table(tableName)
    src_tables = ['mj_sales','mj_weather_info']
    
    target_dfs_list = []
    for src_table in src_tables:
        target_dfs_list.append(snow_df.filter(col("source_table") == src_table))
        
    for table_df in target_dfs_list:
        dim_tables_df = table_df.select('target_table').distinct()

        for dim_table_df in dim_tables_df.collect():
            dim_table = table_df.filter(col("target_table") == dim_table_df[0])

    # # Print a sample of the dataframe to standard output.
    # snow_df.show()

    # Return value will appear in the Results tab.
    return dim_table