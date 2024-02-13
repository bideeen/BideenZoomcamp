-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-413404.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-we/nyc_green_taxi_data.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE de-zoomcamp-413404.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM de-zoomcamp-413404.ny_taxi.external_green_tripdata;


-- Question 1: What is count of records for the 2022 Green Taxi Data??
-- Check external green trip data
SELECT COUNT(*) FROM de-zoomcamp-413404.ny_taxi.external_green_tripdata;
-- Check non_partioned green trip data
SELECT COUNT(*) FROM de-zoomcamp-413404.ny_taxi.green_tripdata_non_partitoned;

-- Question 2:
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT Count(DISTINCT PULocationID) FROM de-zoomcamp-413404.ny_taxi.external_green_tripdata;
SELECT Count(DISTINCT PULocationID) FROM de-zoomcamp-413404.ny_taxi.green_tripdata_partitoned;


-- Question 3:
-- How many records have a fare_amount of 0?
select count(*) 
FROM `de-zoomcamp-413404.ny_taxi.green_tripdata_non_partitoned`
where fare_amount = 0;

-- Question 4:
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE de-zoomcamp-413404.ny_taxi.green_tripdata_partitoned
PARTITION BY
  lpep_pickup_date AS
SELECT * FROM de-zoomcamp-413404.ny_taxi.external_green_tripdata;


-- Question 5. What's the size of the tables?
SELECT DISTINCT PULocationID
FROM `de-zoomcamp-413404.ny_taxi.green_tripdata_non_partitoned`
WHERE DATE(lpep_pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM `de-zoomcamp-413404.ny_taxi.green_tripdata_partitoned`
WHERE DATE(lpep_pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';


