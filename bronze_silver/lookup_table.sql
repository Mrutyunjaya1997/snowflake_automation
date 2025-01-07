CREATE OR REPLACE TABLE demo_db.look_up.mj_lookup (
    source_table     VARCHAR,  -- source table name
    target_table     VARCHAR,  -- 'facts_table' or 'dimension_table'
    column_name      VARCHAR,  -- Column from the parent table
    is_primary_key   VARCHAR,  -- If true, indicates primary key (metadata only)
    transformation   VARCHAR   -- Optional transformation logic, e.g., 'UPPER()'
);

INSERT INTO demo_db.look_up.mj_lookup (source_table, target_table, column_name, is_primary_key, transformation)
VALUES
    -- Customer Dimension
    ('mj_sales','dim_customer_mj','customer_id','TRUE',NULL),
    ('mj_sales','dim_customer_mj','customer_name','FALSE',NULL),
    ('mj_sales','dim_customer_mj','phone_number','FALSE',NULL),
    ('mj_sales','dim_customer_mj','state','FALSE',NULL),

    -- Product Dimension
    ('mj_sales','dim_product_mj','product_id','TRUE',NULL),
    ('mj_sales','dim_product_mj','product_name','FALSE',NULL),
    ('mj_sales','dim_product_mj','product_description','FALSE',NULL),

    -- Transactions Fact
    ('mj_sales','fct_transactions_mj','transaction_id','TRUE',NULL),
    ('mj_sales','fct_transactions_mj','product_id','FALSE',NULL),
    ('mj_sales','fct_transactions_mj','customer_id','FALSE',NULL),
    ('mj_sales','fct_transactions_mj','transaction_date','FALSE',NULL),
    ('mj_sales','fct_transactions_mj','quantity','FALSE',NULL),
    ('mj_sales','fct_transactions_mj','amount','FALSE',NULL);

INSERT INTO demo_db.look_up.mj_lookup (source_table, target_table, column_name, is_primary_key, transformation)
VALUES
    -- Weather Details Dimension
    ('mj_weather_info','dim_weather_details_mj','weather_id','TRUE','foreign key'),
    ('mj_weather_info','dim_weather_details_mj','state','FALSE',NULL),
    ('mj_weather_info','dim_weather_details_mj','temperature','FALSE',NULL),
    ('mj_weather_info','dim_weather_details_mj','humidity','FALSE',NULL),
    ('mj_weather_info','dim_weather_details_mj','weather_condition','FALSE',NULL),

    -- Weather Severity Dimension
    ('mj_weather_info','dim_weather_severity_mj','weather_id','TRUE','foreign key'),
    ('mj_weather_info','dim_weather_severity_mj','weather_severity','FALSE',NULL),
    ('mj_weather_info','dim_weather_severity_mj','wind_speed','FALSE',NULL),
    ('mj_weather_info','dim_weather_severity_mj','precipitation','FALSE',NULL),
    ('mj_weather_info','dim_weather_severity_mj','region','FALSE',NULL),

    -- Weather Fact
    ('mj_weather_info','fct_weather_mj','weather_id','TRUE',NULL),
    ('mj_weather_info','fct_weather_mj','transaction_date','FALSE',NULL);

SELECT * FROM demo_db.look_up.mj_lookup;
SELECT IS_PRIMARY_KEY FROM demo_db.look_up.mj_lookup;


