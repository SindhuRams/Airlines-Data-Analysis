-- Databricks notebook source
-- DBTITLE 1,Creating a table from the csv file
DROP TABLE IF EXISTS airlines;

CREATE TABLE airlines
USING csv
OPTIONS (path "/FileStore/tables/Airlinesdata.csv", header "true")

-- COMMAND ----------

-- DBTITLE 1,Displaying the first few records of table-airlines
SELECT * from airlines limit 5

-- COMMAND ----------

-- DBTITLE 1,Total number of Records in the dataset
select count(*) as Total_Records from airlines;


-- COMMAND ----------

-- DBTITLE 1,Displaying the features(attributes) in table
SHOW COLUMNS FROM airlines;

-- COMMAND ----------

-- DBTITLE 1,Total number of flights for each Airlines
select airline as Airines,count(*) as Total_number_of_flights from airlines group by airline;

-- COMMAND ----------

-- DBTITLE 1,Total number of cancelled flights
select airline as Airlines ,count(*) as Number_of_cancelled_flights from airlines where cancelled="1" group by airline;

-- COMMAND ----------



-- COMMAND ----------

select des_airport as Destination_airports, 
count(*),airline as Airlines 
from airlines where cancelled="1" 
group by des_airport, airline 
order by count(*) desc 
limit 10;

-- COMMAND ----------

-- DBTITLE 1,Top three destinations with maximum cancellations
select DESTINATION_AIRPORT as Destination_airports, 
count(*) as No_of_cancellations 
from airlines where cancelled="1" 
group by DESTINATION_AIRPORT 
order by count(*) desc 
limit 3;

-- COMMAND ----------

-- DBTITLE 1,Airlines of Top 10 Destinations with Maximum Cancellations?
select DESTINATION_AIRPORT as Destination_airports,
airline as Airlines,
count(*) as No_of_cancellations 
from airlines where cancelled="1" 
and DESTINATION_AIRPORT like "ORD"
group by DESTINATION_AIRPORT,airline 
order by count(*) desc 
limit 10;

-- COMMAND ----------

select cancellation_reason as Cancellation_Reason, 
count(*) as Cancellation_Count 
from airlines
where Cancelled = 1 
group by Cancellation_Reason
order by Cancellation_Count Desc
limit 10;


-- COMMAND ----------

SELECT DESTINATION_AIRPORT as Destination_airports,
airline as Airlines,
cancellation_reason,
count(*) as cancellation_count 
FROM airlines
WHERE CANCELLED = 1 and 
DESTINATION_AIRPORT like "ORD" 
GROUP BY DESTINATION_AIRPORT,airline,cancellation_reason
ORDER BY cancellation_count DESC
limit 10;

-- COMMAND ----------

-- DBTITLE 1,Top 10 route(Arrival and Destination) with maximum Cancellations?
SELECT ORIGIN_AIRPORT,DESTINATION_AIRPORT,COUNT(CANCELLED) AS No_Of_Cancellations 
FROM airlines
WHERE CANCELLED = 1 
GROUP BY ORIGIN_AIRPORT,DESTINATION_AIRPORT
ORDER BY No_Of_Cancellations DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Total number of diverted flights
select count(*) as Number_of_diverted_flights from airlines where diverted="1";

-- COMMAND ----------

SELECT des_airport, 
cast(avg(departure_delay) as decimal(10,2)) AS Average_Departure_Delay 
FROM airlines 
GROUP BY des_airport 
ORDER BY avg(departure_delay) desc 
limit 10;

-- COMMAND ----------

SELECT des_airport, 
cast(avg(arrival_delay) as decimal(10,2)) AS Average_Arrival_Delay 
FROM airlines 
GROUP BY des_airport 
ORDER BY avg(arrival_delay) desc 
limit 10;

-- COMMAND ----------

select airline as AIRLINES, 
cast(avg(departure_delay) as decimal(10,2)) as AVG_DEPATURE_DELAY , 
cast(avg(arrival_delay) as decimal(10,2)) as AVG_ARRIVAL_DELAY, 
cast(avg(departure_delay)+avg(arrival_delay) as decimal(10,2)) as AVG_TOTAL_DELAY 
from airlines 
GROUP BY airline 
ORDER BY AVG_TOTAL_DELAY 
desc limit 10;

-- COMMAND ----------

select flight_number as FLIGHT_NUMBER, 
cast(avg(AIR_SYSTEM_DELAY) as decimal(10,2)) as AVG_AIR_SYSTEM_DELAY,
cast(avg(SECURITY_DELAY) as decimal(10,2)) as AVG_SECURITY_DELAY,
cast(avg(AIRLINE_DELAY) as decimal(10,2)) as AVG_AIRLINE_DELAY,
cast(avg(LATE_AIRCRAFT_DELAY) as decimal(10,2)) as AVG_LATE_AIRCRAFT_DELAY,
cast(avg(WEATHER_DELAY) as decimal(10,2)) as AVG_WEATHER_DELAY,
cast(avg(AIR_SYSTEM_DELAY)+avg(SECURITY_DELAY)+avg(AIRLINE_DELAY)+avg(LATE_AIRCRAFT_DELAY)+avg(WEATHER_DELAY) as decimal(10,2)) as TOTAL_DELAY
from airlines
where airline = 'Spirit Air Lines' 
GROUP BY Flight_Number
ORDER BY Total_Delay desc
limit 10;

-- COMMAND ----------

SELECT MONTH,Count(DIVERTED) AS No_of_diversions FROM airlines
WHERE DIVERTED = 1
GROUP BY MONTH 
ORDER BY No_of_diversions DESC ;

-- COMMAND ----------

SELECT MONTH,Count(CANCELLED) AS No_of_cancellations FROM airlines
WHERE CANCELLED = 1 and AIRLINE like "%American Eagle Airlines Inc.%"
GROUP BY MONTH 
ORDER BY No_of_cancellations desc;

-- COMMAND ----------

SELECT MONTH,avg(departure_delay) AS Mean_Departure_Delay FROM airlines
WHERE des_airport like "%Guam International Airport%"
GROUP BY MONTH
ORDER BY avg(departure_delay);

-- COMMAND ----------

SELECT CASE  WHEN day_of_week=1 then 'Sunday' WHEN day_of_week=2 then 'Monday' WHEN day_of_week=3 then 'Tuesday' WHEN day_of_week=4 then 'Wednesday' WHEN day_of_week=5 then 'Thursday' WHEN day_of_week=6 then 'Friday' WHEN day_of_week=7 then 'Saturday' END ,
cast(avg(AIR_SYSTEM_DELAY) as decimal(10,2)) as AVG_AIR_SYSTEM_DELAY,
cast(avg(SECURITY_DELAY) as decimal(10,2)) as AVG_SECURITY_DELAY,
cast(avg(AIRLINE_DELAY) as decimal(10,2)) as AVG_AIRLINE_DELAY,
cast(avg(LATE_AIRCRAFT_DELAY) as decimal(10,2)) as AVG_LATE_AIRCRAFT_DELAY,
cast(avg(WEATHER_DELAY) as decimal(10,2)) as AVG_WEATHER_DELAY,
cast(avg(AIR_SYSTEM_DELAY)+avg(SECURITY_DELAY)+avg(AIRLINE_DELAY)+avg(LATE_AIRCRAFT_DELAY)+avg(WEATHER_DELAY) as decimal(10,2)) as TOTAL_DELAY
from airlines
where airline = 'Spirit Air Lines' 
GROUP BY day_of_week
ORDER BY Total_Delay desc;


-- COMMAND ----------

select month, 
cast(avg(departure_delay) as decimal(10,2)) as AVG_DEPATURE_DELAY , 
cast(avg(arrival_delay) as decimal(10,2)) as AVG_ARRIVAL_DELAY, 
cast(avg(departure_delay)+avg(arrival_delay) as decimal(10,2)) as AVG_TOTAL_DELAY
from airlines 
GROUP BY month 
ORDER BY AVG_TOTAL_DELAY;

-- COMMAND ----------

SELECT day_of_week,avg(departure_delay) AS Mean_Departure_Delay FROM airlines
WHERE des_airport like "%Guam International Airport%"
GROUP BY day_of_week
ORDER BY avg(departure_delay);

-- COMMAND ----------

SELECT MONTH,avg(arrival_delay) AS Mean_Arrival_Delay FROM airlines
WHERE des_airport like "%Wilmington Airport%"
GROUP BY MONTH
ORDER BY avg(arrival_delay);

-- COMMAND ----------

SELECT count(airline_Delay) AS Mean_airline_Delay FROM airlines
where airline_Delay!=0;

-- COMMAND ----------

SELECT MONTH,count(airline_Delay) AS Flights_with_airline_Delay FROM airlines
where airline_Delay!=0
GROUP BY MONTH
ORDER BY month;

-- COMMAND ----------

SELECT Month,airline,avg(airline_Delay) AS Mean_airline_Delay FROM airlines
GROUP BY airline,Month
ORDER BY avg(airline_Delay) desc
limit 10;

-- COMMAND ----------

SELECT MONTH,count(airline_Delay) AS Flights_with_airline_Delay FROM airlines
where airline_Delay!=0
GROUP BY MONTH
ORDER BY month;

-- COMMAND ----------

SELECT MONTH,count(weather_delay) AS Flights_with_weather_Delay FROM airlines
where weather_delay!=0
GROUP BY MONTH
ORDER BY month;

-- COMMAND ----------

SELECT MONTH,count(security_delay) AS Flights_with_security_Delay FROM airlines
where security_delay!=0
GROUP BY MONTH
ORDER BY month;

-- COMMAND ----------

SELECT MONTH,count(air_System_Delay) AS Flights_with_AirSystem_Delay FROM airlines
where air_System_Delay!=0
GROUP BY MONTH
ORDER BY month;

-- COMMAND ----------

SELECT MONTH,Count(CANCELLED) AS No_of_cancellations 
FROM airlines
WHERE CANCELLED = 1
GROUP BY MONTH 
ORDER BY No_of_cancellations DESC ;

