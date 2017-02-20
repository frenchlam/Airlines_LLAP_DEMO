#!/bin/bash
set -e -x;

export Data_DIR=$(pwd)"/data"
export START="2007"
export END="2008"


mkdir -p $Data_DIR
cd $Data_DIR

for YEAR in $( seq $START $END )	
do
	wget -c http://stat-computing.org/dataexpo/2009/$YEAR.csv.bz2
	echo "$YEAR.csv.bz2: OK"
	sleep 1
done

wget -c \
  http://stat-computing.org/dataexpo/2009/airports.csv\
  http://stat-computing.org/dataexpo/2009/carriers.csv\
  http://stat-computing.org/dataexpo/2009/plane-data.csv

echo "Carrier, airport, plane-data : OK"

for each in *.csv
do 
	rm $each.gz || true 
	gzip -1 $each
done

echo "Carrier, airport, plane-data : OK"


#create table structure
cd $Data_DIR

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


