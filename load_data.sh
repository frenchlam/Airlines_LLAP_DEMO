#!/bin/bash
set -e -x;

#create table structure
cd Data_DIR

hive -v -f ./ddl/airline_create.sql
echo "structure created"

LOAD_DATA_FILE="load_data_text.sql"

rm -f ./ddl/$LOAD_DATA_FILE
touch ./ddl/$LOAD_DATA_FILE

for YEAR in $( seq $START $END )
do
	echo "LOAD DATA LOCAL INPATH '$Data_DIR/$YEAR.csv.bz2' INTO TABLE airline_ontime.flights_raw ;" >> ./ddl/$LOAD_DATA_FILE
done

hive -v -f ddl/$LOAD_DATA_FILE
