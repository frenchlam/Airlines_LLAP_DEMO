
drop table if exists flights_raw PURGE;
drop table if exists airports_raw PURGE;
drop table if exists airlines_raw PURGE;
drop table if exists planes_raw PURGE;

create table flights_raw (
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier varchar(5),
  FlightNum int,
  TailNum varchar(8),
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin varchar(3),
  Dest varchar(3),
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode varchar(1),
  Diverted varchar(1),
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
)  
stored as textfile 
tblproperties ("skip.header.line.count"="1")
;

create table airports_raw (
  iata string,
  airport string,
  city string,
  state double,
  country string,
  lat double,
  lon double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
)  
stored as textfile 
tblproperties ("skip.header.line.count"="1")
;

create table airlines_raw (
  code string,
  description string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
)  
stored as textfile 
tblproperties ("skip.header.line.count"="1")
;

create table planes_raw (
  tailnum string,
  owner_type string,
  manufacturer string,
  issue_date string,
  model string,
  status string,
  aircraft_type string,
  engine_type string,
  year int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
 )  
stored as textfile
tblproperties ("skip.header.line.count"="1")
;