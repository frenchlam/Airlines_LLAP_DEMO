
--QUERY CACHE 
set hive.query.results.cache.enabled=true 

select year, month, count(*) 
FROM flights
GROUP BY year, month
ORDER BY year, month


--Materialized view

SELECT origin, count(*) 
FROM flights GROUP BY origin;



CREATE MATERIALIZED VIEW flights_mv
AS 
SELECT year, month, origin, dest, count(*) as num_flights, count(cancelled) as num_cancelled
FROM flights
GROUP BY year, month, origin, dest

select year, month, count(*) 
FROM flights
GROUP BY year, month
ORDER BY year, month


--INFORMATION SCHEMA ; 

--=> show all views across schema ;
select table_schema, table_name, table_type from tables where table_type = "VIEW" ;

--=> top 3 tables 
use sys;
SELECT tbl_name, total_size 
FROM table_stats_view v, tbls t
WHERE t.tbl_id = v.tbl_id ORDER BY cast(v.total_size as int) DESC LIMIT 3;




--JDBC table 

--Create table in postgres
CREATE DATABASE test ;
CREATE USER test WITH PASSWORD 'StrongPassword'
GRANT ALL PRIVILEGES ON DATABASE "test" to test;

CREATE TABLE postgres_table (id INT,  name varchar)
INSERT INTO postgres_table(id, name) VALUES(3, 'titi');


--psql -h mla-hdp3-master.field.hortonworks.com -U test


CREATE EXTERNAL TABLE postgres_table
(  id INT,  name STRING)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
	"hive.sql.database.type" = "POSTGRES",
	"hive.sql.jdbc.driver"="org.postgresql.Driver", 
	"hive.sql.jdbc.url"="jdbc:postgresql://mla-hdp30-0.field.hortonworks.com:5432/test", 
	"hive.sql.dbcp.username"="test",
	"hive.sql.dbcp.password"="StrongPassword",
	"hive.sql.query"="select * from postgres_table",
	"hive.sql.column.mapping" = "id=ID, name=NAME",
	"hive.jdbc.update.on.duplicate" = "true");

