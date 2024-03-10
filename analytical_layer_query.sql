CREATE OR REPLACE TABLE `nyc_tlc_data_analytics.analytics` AS (
SELECT 
f.trip_id,
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.PULocationID,
drop.DOLocationID,
pay.payment_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM 

`nyc_tlc_data_analytics.fact_table` f
JOIN `nyc_tlc_data_analytics.datetime_dim` d  ON f.datetime_id=d.datetime_id
JOIN `nyc_tlc_data_analytics.passenger_count_dim` p  ON p.passenger_count_id=f.passenger_count_id  
JOIN `nyc_tlc_data_analytics.trip_distance_dim` t  ON t.trip_distance_id=f.trip_distance_id  
JOIN `nyc_tlc_data_analytics.rate_code_dim` r ON r.rate_code_id=f.rate_code_id  
JOIN `nyc_tlc_data_analytics.pickup_location_dim` pick ON pick.pickup_location_id=f.pickup_location_id
JOIN `nyc_tlc_data_analytics.dropoff_location_dim` drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN `nyc_tlc_data_analytics.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id);