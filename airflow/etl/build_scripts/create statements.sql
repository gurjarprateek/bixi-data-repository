CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.station_information;

CREATE TABLE IF NOT EXISTS staging.station_information 
( station_id varchar
 ,external_id varchar
 ,name varchar
 ,short_name varchar
 ,lat float
 ,lon float
 ,rental_methods_0 varchar
 ,rental_methods_1 varchar 
 ,capacity int
 ,electric_bike_surcharge_waiver bool
 ,eightd_has_key_dispenser bool
 ,eightd_station_services_0_id varchar 
 ,eightd_station_services_0_service_type varchar
 ,eightd_station_services_0_bikes_availability varchar
 ,eightd_station_services_0_docks_availability varchar
 ,eightd_station_services_0_name varchar
 ,eightd_station_services_0_description varchar
 ,eightd_station_services_0_schedule_description varchar
 ,eightd_station_services_0_link_for_more_info varchar
 ,has_kiosk bool
 ,last_updated int
);

CREATE TABLE IF NOT EXISTS staging.station_status
( station_id varchar
, num_bikes_available int
, num_ebikes_available int
, num_bikes_disabled int
, num_docks_available int
, num_docks_disabled int
, is_installed int
, is_renting int
, is_returning int
, last_reported int
, eightd_has_available_keys bool
, eightd_active_station_services_0_id varchar
, last_updated int
);

CREATE TABLE IF NOT EXISTS staging.trips
(start_date datetime
,start_station_code varchar
,end_date datetime
,end_station_code varchar
,duration_sec int
,is_member int
);