#!/bin/bash
set -e -x;

#create table structure
export Data_DIR=$(pwd)"/data"
export START="2007"
export END="2008"

#create table structure
cd $Data_DIR

#reset
hive -v -f ../ddl/drop_database.sql

#load
hive -v -f ../ddl/airline_create.sql
echo "structure created"

LOAD_DATA_FILE="load_data_text.sql"

rm -f ../ddl/$LOAD_DATA_FILE
touch ../ddl/$LOAD_DATA_FILE

echo "LOAD DATA LOCAL INPATH '$Data_DIR/carriers.csv.gz' INTO TABLE airline_ontime.airlines_raw;" >> ../ddl/$LOAD_DATA_FILE
echo "LOAD DATA LOCAL INPATH '$Data_DIR/airports.csv.gz' INTO TABLE airline_ontime.airports_raw;" >> ../ddl/$LOAD_DATA_FILE
echo "LOAD DATA LOCAL INPATH '$Data_DIR/plane-data.csv.gz' INTO TABLE airline_ontime.planes_raw;" >> ../ddl/$LOAD_DATA_FILE


for YEAR in $( seq $START $END )
do
	echo "LOAD DATA LOCAL INPATH '$Data_DIR/$YEAR.csv.bz2' INTO TABLE airline_ontime.flights_raw ;" >> ../ddl/$LOAD_DATA_FILE
done

echo "loading data"
hive -v -f ../ddl/$LOAD_DATA_FILE

echo "to ORC"
hive -v -f ../ddl/to_orc.sql
