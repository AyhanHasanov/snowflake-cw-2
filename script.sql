CREATE DATABASE IF NOT EXISTS zebra_shop;
USE zebra_shop;

CREATE SCHEMA IF NOT EXISTS manager_toolset;
USE SCHEMA manager_toolset;

CREATE FILE FORMAT IF NOT EXISTS json_format
  TYPE = JSON;

CREATE STAGE IF NOT EXISTS stg_data
  FILE_FORMAT = json_format
  COMMENT = 'This is the staging area';

LIST @stg_data;

CREATE TEMPORARY TABLE IF NOT EXISTS raw_customers_json (
  id INT, 
  json_payload VARIANT
);

CREATE TEMPORARY TABLE IF NOT EXISTS raw_orders_json (
  id INT, 
  json_payload VARIANT
);

INSERT INTO raw_customers_json
SELECT 1, $1
FROM @stg_data/customers_data.json;

INSERT INTO raw_orders_json
SELECT 1, $1
FROM @stg_data/orders_data.json;

CREATE SCHEMA IF NOT EXISTS transformed;
USE SCHEMA transformed;

CREATE TABLE td_customers AS
SELECT
  value:"customer_id"::STRING AS customer_id,
  value:"name"::STRING AS name,
  value:"email"::STRING AS email,
  value:"registration_date"::DATE AS registration_date,
  value:"address" AS address,
  value:"loyalty_points"::FLOAT AS loyalty_points
FROM manager_toolset.raw_customers_json,
LATERAL FLATTEN(INPUT => json_payload);

CREATE TABLE td_orders AS
SELECT
  value:"order_id"::STRING AS order_id,
  value:"customer_id"::STRING AS customer_id,
  value:"order_date"::DATE AS order_date,
  value:"total_amount"::FLOAT AS total_amount,
  value:"items" AS items,
  value:"shipping_method"::STRING AS shipping_method
FROM manager_toolset.raw_orders_json,
LATERAL FLATTEN(INPUT => json_payload);

CREATE TABLE td_order_items AS
SELECT DISTINCT
  o.order_id,
  f.value:"product_id"::STRING AS product_id,
  f.value:"name"::STRING AS name,
  f.value:"quantity"::INT AS quantity, 
  f.value:"price"::FLOAT AS price
FROM td_orders o,
LATERAL FLATTEN(INPUT => items) f;

CREATE TABLE td_registered_users AS
SELECT COUNT(0) AS count
FROM td_customers;

CREATE TABLE td_sold_items AS
SELECT 
  COUNT(product_id) AS number_of_products,
  SUM(price) AS total_price
FROM td_order_items;
