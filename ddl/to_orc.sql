use airline_ontime;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;


drop table if exists flights;
drop table if exists airports;
drop table if exists airlines;
drop table if exists planes;

create table airports (
  iata string,
  airport string,
  city string,
  state double,
  country string,
  lat double,
  lon double
)
STORED AS ORC
;

create table airlines (
  code string,
  description string
)
STORED AS ORC
;

create table planes (
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
STORED AS ORC
;

insert overwrite table airports select * from airports_raw;
insert overwrite table airlines select * from airlines_raw;
insert overwrite table planes select * from planes_raw;

create table flights (
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
) 
PARTITIONED BY (Year int)
STORED AS ORC
TBLPROPERTIES("orc.bloom.filter.columns"= "Month,DayOfWeek,DayofMonth,UniqueCarrier,TailNum,FlightNum,Origin,Dest,CancellationCode",
  "orc.create.index"="true");

insert overwrite table flights partition(Year) 
select
  Month,
  DayofMonth,
  DayOfWeek,
  DepTime ,
  CRSDepTime,
  ArrTime,
  CRSArrTime,
  UniqueCarrier,
  FlightNum,
  TailNum,
  ActualElapsedTime,
  CRSElapsedTime,
  AirTime,
  ArrDelay,
  DepDelay,
  Origin,
  Dest,
  Distance,
  TaxiIn,
  TaxiOut,
  Cancelled,
  CancellationCode,
  Diverted,
  CarrierDelay,
  WeatherDelay,
  NASDelay,
  SecurityDelay,
  LateAircraftDelay,
  Year
from flights_raw
Distribute By Year
Sort by concat(Month,DayofMonth) ;

