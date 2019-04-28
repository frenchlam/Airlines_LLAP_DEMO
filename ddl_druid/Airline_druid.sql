use airline_ontime ; 

--set hive.druid.metadata.username=druid;
--set hive.druid.metadata.password=StrongPassword;
--set hive.druid.metadata.uri=jdbc:postgresql://mla-hdp30-0.field.hortonworks.com:5432/druid;
--set hive.druid.indexer.partition.size.max=1000000;
--set hive.druid.indexer.memory.rownum.max=100000;
--set hive.druid.passiveWaitTimeMs=180000;
--set hive.druid.storage.storageDirectory=/apps/hive/warehouse;
--set hive.druid.passiveWaitTimeMs=180000;

--set hive.query.results.cache.enabled=false

--partial 
CREATE EXTERNAL TABLE flights_lim
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
	"druid.segment.granularity" = "YEAR",
	"druid.query.granularity" = "HOUR")
AS 
SELECT 
  cast(flights.Year || '-' || flights.Month || '-' || flights.DayofMonth || ' ' || floor(flights.CRSDepTime/100) || ':00:00' AS timestamp) as `__time`,
  cast(flights.Year as string) as `Year` ,
  cast(flights.Month as string) as `Month` ,
  cast(flights.DayofMonth as string) as `DayofMonth`,
  cast(flights.DayOfWeek as string) as `DayOfWeek`,
  cast(floor(flights.CRSDepTime/100) as string) as `DepTime_hour`,
  cast(pmod(flights.CRSDepTime,100) as string) as `DepTime_minute`,
  cast(floor(flights.CRSArrTime/100) as string) as `ArrTime_hour`,
  cast(pmod(flights.CRSArrTime,100) as string) as `ArrTime_minute`,
  airlines.description as airline_name,
  cast(flights.FlightNum as string) as `FlightNum`,
  cast(flights.TailNum as string) as `TailNum`,
  IF(flights.AirTime IS NULL OR flights.AirTime < 0, -1,flights.AirTime) as `AirTime` ,
  cast(flights.Origin as string) as Origin,
  air_origin.airport as Origin_airport,
  air_origin.city as Origin_city,
  cast(flights.Dest as string) as Dest,
  air_dest.airport as Dest_airport,
  air_dest.city as Dest_city,
  flights.Distance ,
  flights.TaxiIn ,
  flights.TaxiOut ,
  flights.Cancelled ,
  flights.Diverted ,
  flights.CarrierDelay ,
  flights.WeatherDelay ,
  flights.SecurityDelay
FROM 
  flights, airlines, airports air_origin, airports air_dest
WHERE
 (airlines.code = flights.UniqueCarrier) 
 AND (air_origin.iata = flights.Origin) 
 AND (air_dest.iata = flights.Origin)
 AND (flights.month = 1) AND (flights.year = 1989)






-- Complete 

CREATE EXTERNAL TABLE airlines_druid
	(`__time` TIMESTAMP, 
	Year STRING, 
	Month STRING, 
	DayofMonth STRING,
	DayOfWeek STRING,
	DepTime_hour STRING,
	DepTime_minute STRING, 
	ArrTime_hour STRING, 
	ArrTime_minute STRING,  
	airline_name STRING, 
	FlightNum STRING, 
	TailNum STRING, 
	AirTime INT, 
	Origin STRING, 
	Origin_airport STRING, 
	Origin_city STRING, 
	Dest STRING, 
	Dest_airport STRING, 
	Dest_city STRING, 
	Distance INT,
	TaxiIn INT,
	TaxiOut INT,
	Cancelled INT,
	Diverted  INT, 
	CarrierDelay INT,
	WeatherDelay INT,
	SecurityDelay INT)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
	"druid.segment.granularity" = "YEAR",
	"druid.query.granularity" = "HOUR")

INSERT OVERWRITE TABLE airlines_druid
SELECT 
  cast(flights.Year || '-' || flights.Month || '-' || flights.DayofMonth || ' ' || floor(flights.CRSDepTime/100) || ':00:00' AS timestamp) as `__time`,
  cast(flights.Year as string) as `Year` ,
  cast(flights.Month as string) as `Month` ,
  cast(flights.DayofMonth as string) as `DayofMonth`,
  cast(flights.DayOfWeek as string) as `DayOfWeek`,
  cast(floor(flights.CRSDepTime/100) as string) as `DepTime_hour`,
  cast(pmod(flights.CRSDepTime,100) as string) as `DepTime_minute`,
  cast(floor(flights.CRSArrTime/100) as string) as `ArrTime_hour`,
  cast(pmod(flights.CRSArrTime,100) as string) as `ArrTime_minute`,
  airlines.description as airline_name,
  cast(flights.FlightNum as string) as `FlightNum`,
  cast(flights.TailNum as string) as `TailNum`,
  IF(flights.AirTime IS NULL OR flights.AirTime < 0, -1,flights.AirTime) as `AirTime` ,
  cast(flights.Origin as string) as Origin,
  air_origin.airport as Origin_airport,
  air_origin.city as Origin_city,
  cast(flights.Dest as string) as Dest,
  air_dest.airport as Dest_airport,
  air_dest.city as Dest_city,
  flights.Distance ,
  flights.TaxiIn ,
  flights.TaxiOut ,
  flights.Cancelled ,
  flights.Diverted ,
  flights.CarrierDelay ,
  flights.WeatherDelay ,
  flights.SecurityDelay
FROM 
  flights, airlines, airports air_origin, airports air_dest
WHERE
 (airlines.code = flights.UniqueCarrier) AND (air_origin.iata = flights.Origin) AND (air_dest.iata = flights.Origin)


-- *** Materialized view 

CREATE MATERIALIZED VIEW flights_mv_druid1 
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
AS
SELECT floor(cast(flights.Year || '-' || flights.Month || '-' || flights.DayofMonth || ' ' || floor(flights.CRSDepTime/100) || ':00:00' AS timestamp) to month) AS `__time`, dest, origin, count(*) AS `num_flights`, sum(flights.cancelled) AS `num_cancelled` 
FROM  flights
GROUP BY floor(cast(flights.Year || '-' || flights.Month || '-' || flights.DayofMonth || ' ' || floor(flights.CRSDepTime/100) || ':00:00' AS timestamp) to month), dest, origin


CREATE MATERIALIZED VIEW flights_mv
AS 
SELECT year, month, origin, dest, count(*) as num_flights, count(cancelled) as num_cancelled
FROM flights
GROUP BY year, month, origin, dest

-- new query 
select year, month, origin, count(cancelled) as num_cancelled
from flights 
where origin = "ATL"
group by year, month, origin 
order by year, month



