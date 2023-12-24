---creat the data base schema 

SELECT * FROM orders_table
SELECT MAX(LENGTH(date_uuid)) AS max_char_number
FROM orders_table;

ALTER TABLE orders_table
  ALTER COLUMN date_uuid TYPE TEXT,
  ALTER COLUMN user_uuid TYPE TEXT,
  ALTER COLUMN card_number TYPE VARCHAR(36),
  ALTER COLUMN store_code TYPE VARCHAR(36),
  ALTER COLUMN product_code TYPE VARCHAR(36),
  ALTER COLUMN product_quantity TYPE SMALLINT;

-- Once altered to TEXT, try converting them explicitly to UUID after ensuring data compatibility
ALTER TABLE orders_table
  ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
  ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

SELECT * FROM dim_store_details
SELECT MAX(LENGTH(address)) AS max_char_length
FROM dim_store_details;
ALTER TABLE dim_store_details
     ALTER COLUMN   longitude     TYPE    TEXT,
     ALTER COLUMN   locality      TYPE    VARCHAR(255),
     ALTER COLUMN   store_code    TYPE    VARCHAR(255),
     ALTER COLUMN   staff_numbers TYPE    TEXT,
     ALTER COLUMN   opening_date  TYPE    TEXT,
     ALTER COLUMN   store_type    TYPE    VARCHAR(255),
     ALTER COLUMN   latitude      TYPE    TEXT,
     ALTER COLUMN   country_code  TYPE    VARCHAR(10),
     ALTER COLUMN   continent     TYPE    VARCHAR(255);

ALTER TABLE dim_store_details
  ALTER COLUMN longitude  TYPE FLOAT USING longitude::FLOAT,
  ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
  ALTER COLUMN  latitude TYPE FLOAT USING  latitude::FLOAT,
  ALTER COLUMN  opening_date TYPE DATE USING  opening_date::DATE;

---REPLACE £ WITH ''

UPDATE dim_products
   SET product_price = REPLACE (product_price, '£' ,'')
   WHERE product_price LIKE '£%';
   
  --- remove Kg unite  
UPDATE dim_products
  SET weight = REPLACE (weight,'kg','')
  WHERE weight LIKE '%kg';
--- rename weight column 
ALTER TABLE dim_products
  RENAME COLUMN weight TO "weight(kg)";

---ADD a New column weight_class 
ALTER TABLE dim_products
  ADD COLUMN weight_class  VARCHAR(50);
--Alter data type of weight(kg)
ALTER TABLE dim_products
 ALTER COLUMN "weight(kg)" TYPE FLOAT USING "weight(kg)"::FLOAT ;
---Update 'weight_class' based on 'weight' column conditions
UPDATE dim_products
 SET weight_class=
  CASE
   WHEN "weight(kg)" < 2 THEN 'Light'
   WHEN "weight(kg)" >= 2 AND "weight(kg)" < 40 THEN 'Mid-Sized'
   WHEN "weight(kg)" >= 140 AND "weight(kg)" < 140 THEN  'Heavy'
   WHEN "weight(kg)" >= 140 THEN 'Truck_required'
   ELSE 'Unknowen'
END;



SELECT * FROM dim_products
SELECT * FROM dim_users
SELECT * FROM dim_date_times
SELECT * FROM dim_store_details
SELECT * FROM orders_table
SELECT * FROM dim_card_details
SELECT * FROM orders_table
SELECT * FROM dim_users
ALTER TABLE dim_store_details
DROP COLUMN lat;

SELECT store_type FROM dim_store_details
WHERE store_type LIKE 'web';


---RENAME removed column
ALTER TABLE dim_products
 RENAME COLUMN removed TO "still_available" ;
--- Alter the data type of dim_products
ALTER TABLE dim_products 
 ALTER COLUMN product_price TYPE TEXT,
 ---ALTER COLUMN EAN TYPE VARCHAR(20),
 ALTER COLUMN product_code TYPE VARCHAR(20),
 ALTER COLUMN date_added TYPE TEXT,
 ALTER COLUMN uuid TYPE TEXT,
 ALTER COLUMN still_available TYPE TEXT,
 ALTER COLUMN weight_class TYPE VARCHAR(20);

--- convert data types explicitly 
ALTER TABLE dim_products
 ALTER COLUMN date_added TYPE DATE USING date_added :: DATE,
 ALTER COLUMN uuid TYPE UUID USING uuid :: UUID,
 ALTER COLUMN product_price TYPE FLOAT USING product_price :: FLOAT;
 --ALTER COLUMN "EAN" TYPE VARCHAR(20) USING EAN :: VARCHAR(20);
 --ALTER COLUMN still_available TYPE BOOLEAN USING still_available :: BOOLEAN;



--- Update the dim_datetime table 
ALTER TABLE dim_date_times

 ALTER COLUMN month TYPE VARCHAR(20) USING month :: VARCHAR(20),
 ALTER COLUMN year TYPE VARCHAR(20) USING year :: VARCHAR(20),
 ALTER COLUMN day TYPE  VARCHAR(20) USING day :: VARCHAR(20),
 ALTER COLUMN time_period TYPE VARCHAR(15) USING time_period :: VARCHAR(15);
 ALTER TABLE dim_date_times
 ALTER COLUMN date_uuid TYPE UUID USING date_uuid :: UUID;


---Alter dim card detail table 
ALTER TABLE dim_card_details
 ALTER COLUMN  card_number TYPE VARCHAR(255),
 ALTER COLUMN expiry_date TYPE VARCHAR(255),
 ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed :: DATE;

SELECT date_payment_confirmed FROM dim_card_details
WHERE date_payment_confirmed LIKE 'NULL';

--- ADD primary keys for dim_products
ALTER TABLE dim_products
  ADD PRIMARY KEY (product_code);

---add primary key for dim_user table 
ALTER TABLE dim_users
 ADD PRIMARY KEY (user_uuid);


---add primary key dim_store table
ALTER TABLE dim_date_times
 ADD PRIMARY KEY (date_uuid);

---add primary key 
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

---add primary key dim_store_details
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

---creat a foraign key in Order table 
ALTER TABLE orders_table
ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
ADD CONSTRAINT fk_order_status_id FOREIGN KEY (order_status_id) REFERENCES dim_order_status(order_status_id);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code) 
REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);





  ---a code to find the primary key of any table 
  SELECT
    pg_attribute.attname AS column_name
FROM
    pg_index,
    pg_class,
    pg_attribute,
    pg_namespace
WHERE
    pg_class.oid = 'dim_store_details'::regclass AND
    indrelid = pg_class.oid AND
    nspname = 'public' AND
    pg_class.relnamespace = pg_namespace.oid AND
    pg_attribute.attrelid = pg_class.oid AND
    pg_attribute.attnum = any(pg_index.indkey)
    AND indisprimary;

---countries with highest number of store 
SELECT sd.country_code, COUNT(sd.store_code) AS store_count
FROM dim_store_details sd
GROUP BY sd.country_code
ORDER BY store_count DESC
LIMIT 3;
---locations with highest number of store 
SELECT sd.locality,COUNT(store_code) AS store_count
FROM dim_store_details sd 
GROUP BY sd.locality
ORDER BY store_count DESC
LIMIT 7;

---months that have produced the most sales

SELECT ot.product_quantity,
       ot.product_code,
       ot.date_uuid,
       pd.total_price
FROM orders_table ot
INNER JOIN (
  SELECT pd.product_code,
         SUM(product_price * ot.product_quantity) AS total_price
  FROM dim_products pd
  INNER JOIN orders_table ot ON pd.product_code = ot.product_code
  GROUP BY pd.product_code
  ORDER BY total_price DESC
) pd ON ot.product_code = pd.product_code;

SELECT 
    DATE_TRUNC('month', ot.date_uuid) AS month_year,
    SUM(dp.product_price) AS total_product_price,
    COUNT(*) AS total_sales
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_date_times dt ON ot.date_uuid = dt.date_uuid
GROUP BY 
    month_year
ORDER BY 
    total_sales DESC;
---Find which months in which years have had the most sales historically.
SELECT 
    ddt.month,
    ddt.year,
    SUM(dp.product_price) AS total_product_price,
    COUNT(*) AS total_sales
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
GROUP BY 
    ddt.month,ddt.year
ORDER BY 
    total_sales DESC;

  
  --the total and percentage of sales coming from each of the different store types.
  SELECT 
    sd.store_type,
    SUM(dp.product_price) AS total_price,
    (SUM(dp.product_price) / SUM(SUM(dp.product_price)) OVER ()) * 100 AS percentage_total_price,
    COUNT(*) AS total_sales

  FROM 
    orders_table ot
  JOIN 
     dim_products dp ON ot.product_code=dp.product_code
  JOIN
     dim_store_details sd ON ot.store_code=sd.store_code
  GROUP BY 
      sd.store_type
  ORDER BY percentage_total_price;
---Find which months in which years have had the most sales historically.
  SELECT
      dt.year,
      MAX(dt.month) AS top_sales_month,
      SUM(dp.product_price) AS total_price,
      COUNT(*) AS total_sales
  FROM 
     orders_table ot
  JOIN 
    dim_products dp ON dp.product_code=ot.product_code
  JOIN 
     dim_date_times dt ON dt.date_uuid=ot.date_uuid
  GROUP BY 
       dt.year,
       dt.month
       
  ORDER BY 
         total_price DESC;

  
  
---a query to determine the staff numbers in each of the countries the company sells in.
SELECT 
    st.country_code,
    SUM(st.staff_numbers) AS total_staff_numbers
FROM 
    dim_store_details st
GROUP BY
    st.country_code
ORDER BY
    total_staff_numbers DESC;

---Determine which type of store is generating the most sales in Germany.
SELECT 
    st.store_type,
    st.country_code,
    SUM(pd.product_price * ot.product_quantity) AS total_sales
FROM 
   orders_table ot 
JOIN 
   dim_products pd ON pd.product_code=ot.product_code
JOIN 
   dim_store_details st ON st.store_code=ot.store_code
WHERE 
     st.country_code LIKE 'DE'
GROUP BY
    st.store_type,
    st.country_code
ORDER BY
     total_sales;

---Determine the average time taken between each sale grouped by year

SELECT 
    EXTRACT(YEAR FROM dt.timestamp) AS year,
    AVG((LEAD(dt.timestamp) OVER (PARTITION BY EXTRACT(YEAR FROM dt.timestamp) ORDER BY dt.timestamp) - dt.timestamp)::interval) AS actual_time_taken
FROM 
    dim_date_times dt
JOIN 
    orders_table ot ON ot.date_uuid = dt.date_uuid
GROUP BY 
    year
ORDER BY 
    year;
    






      
      
      
      
      
      
   

