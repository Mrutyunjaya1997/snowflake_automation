-- Create MJ_SALES table
CREATE OR REPLACE TABLE demo_db.bronze.MJ_SALES (
    transaction_id NUMBER,
    product_id VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    phone_number VARCHAR,
    state VARCHAR,
    product_name VARCHAR,
    product_description VARCHAR,
    transaction_date DATE,
    quantity NUMBER,
    amount NUMBER
);

-- Insert data into MJ_SALES
INSERT INTO demo_db.bronze.MJ_SALES (
    transaction_id, product_id, customer_id, customer_name, phone_number, 
    state, product_name, product_description, transaction_date, quantity, amount
)
VALUES
    (1,'P001','C001','Alice Johnson','9876543210','California','Laptop','15-inch laptop with 8GB RAM','2024-11-01',1,1200.00),
    (2,'P002','C002','Bob Smith','8765432109','Texas','Smartphone','Latest Android smartphone','2024-11-02',2,1500.00),
    (3,'P003','C003','Charlie Brown','7654321098','New York','Tablet','10-inch tablet with 64GB storage','2024-11-03',1,500.00),
    (4,'P004','C004','Diana Prince','6543210987','Florida','Smartwatch','Fitness smartwatch with GPS','2024-11-04',3,600.00),
    (5,'P005','C005','Evan Turner','5432109876','Illinois','Monitor','27-inch 4K UHD monitor','2024-11-05',1,300.00),
    (6,'P006','C006','Fiona Davis','4321098765','Ohio','Headphones','Noise-cancelling wireless headphones','2024-11-06',1,200.00),
    (7,'P007','C007','George Hill','3210987654','Nevada','Keyboard','Mechanical gaming keyboard','2024-11-07',4,400.00),
    (8,'P008','C008','Hannah Clark','2109876543','Colorado','Mouse','Ergonomic wireless mouse','2024-11-08',5,150.00),
    (9,'P009','C009','Ian Wright','1098765432','Arizona','Router','High-speed Wi-Fi router','2024-11-09',1,120.00),
    (10,'P010','C010','Jane Foster','9876543211','Washington','External Drive','1TB portable external drive','2024-11-10',2,250.00),
    (11,'P011','C011','Kevin Parker','8765432108','Georgia','Laptop','14-inch laptop with 16GB RAM','2024-11-11',1,1400.00),
    (12,'P012','C012','Laura White','7654321097','Oregon','Printer','All-in-one wireless printer','2024-11-12',1,180.00),
    (13,'P013','C013','Mike Ross','6543210986','Utah','Camera','Mirrorless digital camera','2024-11-13',1,900.00),
    (14,'P014','C014','Nina Scott','5432109875','Michigan','Tripod','Lightweight camera tripod','2024-11-14',3,90.00),
    (15,'P015','C015','Oscar Grant','4321098764','Virginia','Flash Drive','128GB USB 3.0 flash drive','2024-11-15',10,150.00),
    (16,'P016','C016','Pamela Adams','3210987653','Minnesota','Speakers','Bluetooth portable speakers','2024-11-16',1,120.00),
    (17,'P017','C017','Quincy Blake','2109876542','Kentucky','Charger','Fast-charging wall adapter','2024-11-17',3,75.00),
    (18,'P018','C018','Rachel Green','1098765431','Indiana','Power Bank','20,000mAh portable power bank','2024-11-18',2,80.00),
    (19,'P019','C019','Sam Wilson','9876543212','Nevada','Webcam','HD webcam with microphone','2024-11-19',1,60.00),
    (20,'P020','C020','Tina Turner','8765432107','Texas','Desk Lamp','LED desk lamp with dimmer','2024-11-20',2,50.00);

-- Create MJ_WEATHER_INFO table
CREATE OR REPLACE TABLE demo_db.bronze.MJ_WEATHER_INFO (
    weather_id NUMBER PRIMARY KEY,            -- Unique ID for weather data
    transaction_date DATE,        -- Date of transaction
    state VARCHAR,                -- State where the transaction occurred
    temperature DECIMAL(5,2),     -- Temperature (Celsius)
    humidity DECIMAL(5,2),        -- Humidity percentage
    weather_condition VARCHAR,    -- Description (Sunny, Rainy, etc.)
    wind_speed DECIMAL(5,2),      -- Wind speed (km/h)
    precipitation DECIMAL(5,2),   -- Precipitation (mm)
    weather_severity VARCHAR,     -- Severity (Mild, Severe, etc.)
    region VARCHAR                -- Region classification (e.g., West)
);

-- Insert data into MJ_WEATHER_INFO
INSERT INTO demo_db.bronze.MJ_WEATHER_INFO (
    weather_id, transaction_date, state, temperature, humidity, 
    weather_condition, wind_speed, precipitation, weather_severity, region
)
VALUES
    (1,'2024-11-01','California',22.5,60.0,'Sunny',5.0,0.0,'Mild','West'),
    (2,'2024-11-02','Texas',25.0,55.0,'Cloudy',8.0,0.1,'Mild','South'),
    (3,'2024-11-03','New York',15.5,70.0,'Rainy',12.0,5.0,'Severe','East'),
    (4,'2024-11-04','Florida',28.0,80.0,'Sunny',6.0,0.0,'Mild','South'),
    (5,'2024-11-05','Illinois',10.0,65.0,'Snowy',15.0,2.0,'Severe','Midwest'),
    (6,'2024-11-06','Ohio',12.0,72.0,'Cloudy',10.0,0.5,'Moderate','Midwest'),
    (7,'2024-11-07','Nevada',20.0,50.0,'Sunny',4.0,0.0,'Mild','West'),
    (8,'2024-11-08','Colorado',8.0,85.0,'Rainy',20.0,10.0,'Severe','West'),
    (9,'2024-11-09','Arizona',30.0,40.0,'Sunny',3.0,0.0,'Mild','West'),
    (10,'2024-11-10','Washington',15.0,78.0,'Rainy',9.0,3.0,'Moderate','West');

-- View the inserted data
SELECT * FROM demo_db.bronze.MJ_SALES;
SELECT * FROM demo_db.bronze.MJ_WEATHER_INFO;

select get_ddl('table','DEMO_DB.SILVER.FCT_TRANSACTIONS_MJ');
