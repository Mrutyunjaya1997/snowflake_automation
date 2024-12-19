CREATE OR REPLACE TABLE demo_db.lookup.mj_lookup (
        source_table VARCHAR,
        -- source table name
        target_table VARCHAR,
        -- 'facts_table' or 'dimension_table'
        column_name VARCHAR,
        -- Column from the parent table
        is_primary_key VARCHAR,
        -- If true, indicates the primary key (useful for dimensions)
        transformation VARCHAR -- Optional transformation logic, e.g., 'UPPER()'
    );
INSERT INTO demo_db.lookup.mj_lookup (
        source_table,
        target_table,
        column_name,
        is_primary_key,
        transformation
    )
VALUES (
        'mj_sales',
        'dim_customer_mj',
        'customer_id',
        'TRUE',
        NULL
    ),
    (
        'mj_sales',
        'dim_customer_mj',
        'customer_name',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'dim_customer_mj',
        'phone_number',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'dim_customer_mj',
        'state',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'dim_product_mj',
        'product_id',
        'TRUE',
        NULL
    ),
    (
        'mj_sales',
        'dim_product_mj',
        'product_name',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'dim_product_mj',
        'product_description',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'fct_transactions_mj',
        'transaction_id',
        'TRUE',
        NULL
    ),
    (
        'mj_sales',
        'fct_transactions_mj',
        'product_id',
        'FALSE',
        'foreign key'
    ),
    (
        'mj_sales',
        'fct_transactions_mj',
        'customer_id',
        'FALSE',
        'foreign key'
    ),
    (
        'mj_sales',
        'fct_transactions_mj',
        'transaction_date',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'fct_transactions_mj',
        'quantity',
        'FALSE',
        NULL
    ),
    (
        'mj_sales',
        'fct_transactions_mj',
        'amount',
        'FALSE',
        NULL
    );
INSERT INTO demo_db.lookup.mj_lookup (
        source_table,
        target_table,
        column_name,
        is_primary_key,
        transformation
    )
VALUES (
        'mj_weather_info',
        'dim_weather_details_mj',
        'weather_id',
        'TRUE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_details_mj',
        'state',
        'FALSE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_details_mj',
        'temperature',
        'FALSE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_details_mj',
        'humidity',
        'FALSE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_details_mj',
        'weather_condition',
        'FALSE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_severity_mj',
        'weather_id',
        'TRUE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_severity_mj',
        'weather_severity',
        'FALSE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_severity_mj',
        'wind_speed',
        'FALSE',
        NULL
    ),
    (
        'mj_weather_info',
        'dim_weather_severity_mj',
        'precipitation',
        'FALSE',
        'foreign key'
    ),
    (
        'mj_weather_info',
        'dim_weather_severity_mj',
        'region',
        'FALSE',
        'foreign key'
    ),
    (
        'mj_weather_info',
        'fct_weather_mj',
        'weather_id',
        'TRUE',
        NULL
    ),
    (
        'mj_weather_info',
        'fct_weather_mj',
        'transaction_date',
        'FALSE',
        NULL
    )
select *
from demo_db.lookup.mj_lookup;
select IS_PRIMARY_KEY
from demo_db.lookup.mj_lookup;