## step1 create postgres_conn
## step2 create table by loginning to psql

psql -d postgres -U airflow 

SELECT
	datname
FROM
	pg_database;


CREATE TABLE IF NOT EXISTS clean_store_transactions(
STORE_ID VARCHAR (50), 
STORE_LOCATION VARCHAR(50), 
PRODUCT_CATEGORY VARCHAR(50), 
PRODUCT_ID integer, 
MRP float, CP float8, 
DISCOUNT float8, 
SP float8,
DATE date);

SELECT
	*
FROM
	clean_store_transactions;