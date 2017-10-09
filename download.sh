#!/bin/bash
set -e -x;

export Data_DIR=$(pwd)"/data"
export HDFS_DIR="/tmp/airline_demo/data"
export START="1988"
export END="2008"
export DATABASE="airline_ontime"


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

#Download weather data 
wget -c https://drive.google.com/open?id=0BzgswUsgqpZSczdfbUxtdm94QU0 \ 
https://drive.google.com/open?id=0BzgswUsgqpZSa05haEhsdEMtOUk


#create table structure
cd $Data_DIR

tail -n+3 ../ddl/airline_create.sql > ../ddl/tmp_file_create ; mv ../ddl/tmp_file_create ../ddl/airline_create.sql
sed -i "1 i\use ${DATABASE};" ../ddl/airline_create.sql
sed -i "1 i\create database if not exists ${DATABASE};" ../ddl/airline_create.sql


#hive -v -f ../ddl/airline_create.sql
echo "structure created"


###### load data
#create sql load file 
LOAD_DATA_FILE="load_data_text.sql"

rm -f ../ddl/$LOAD_DATA_FILE
touch ../ddl/$LOAD_DATA_FILE

#echo "LOAD DATA LOCAL INPATH '$Data_DIR/carriers.csv.gz' INTO TABLE $DATABASE.airlines_raw;" >> ../ddl/$LOAD_DATA_FILE
#echo "LOAD DATA LOCAL INPATH '$Data_DIR/airports.csv.gz' INTO TABLE $DATABASE.airports_raw;" >> ../ddl/$LOAD_DATA_FILE
#echo "LOAD DATA LOCAL INPATH '$Data_DIR/plane-data.csv.gz' INTO TABLE $DATABASE.planes_raw;" >> ../ddl/$LOAD_DATA_FILE


#for YEAR in $( seq $START $END )
#do
#	echo "LOAD DATA LOCAL INPATH '$Data_DIR/$YEAR.csv.bz2' INTO TABLE $DATABASE.flights_raw ;" >> ../ddl/$LOAD_DATA_FILE
#done

###### Push data to hdfs 
##create dir
sudo -u hdfs hdfs dfs -rmdir --ignore-fail-on-non-empty /tmp/airline_raw 
sudo -u hdfs hdfs dfs -mkdir /tmp/airline_raw
##Push to hdfs 
sudo -u hdfs hdfs dfs -fromlocal $Data_DIR/* $HDFS_DIR
sudo -u hdfs hdfs dfs -chmod -R 777 $HDFS_DIR
sudo -u hdfs hdfs dfs -chown -R hive:hdfs $HDFS_DIR

echo "LOAD DATA INPATH '$HDFS_DIR/carriers.csv.gz' INTO TABLE $DATABASE.airlines_raw;" >> ../ddl/$LOAD_DATA_FILE
echo "LOAD DATA INPATH '$HDFS_DIR/airports.csv.gz' INTO TABLE $DATABASE.airports_raw;" >> ../ddl/$LOAD_DATA_FILE
echo "LOAD DATA INPATH '$HDFS_DIR/plane-data.csv.gz' INTO TABLE $DATABASE.planes_raw;" >> ../ddl/$LOAD_DATA_FILE


for YEAR in $( seq $START $END )
do
	echo "LOAD DATA INPATH '$HDFS_DIR/$YEAR.csv.bz2' INTO TABLE $DATABASE.flights_raw ;" >> ../ddl/$LOAD_DATA_FILE
done



#load data 
echo "loading data"
hive -v -f ../ddl/$LOAD_DATA_FILE


