#!/bin/bash
set -e -x;

#Default values
export Data_DIR="./data"
export HDFS_DIR="/tmp/airline_demo"

export START="1988"
export END="2008"

export DATABASE="airline_ontime"
export HIVE_PROTOCOL="http"  # binary | http
export LLAP=false
export HIVE_PORT_BINARY=10000
export LLAP_PORT_BINARY=10500
export HIVE_PORT_HTTP=10001
export LLAP_PORT_HTTP=10501
export HIVE_HOST="localhost"

#overide defaults
source config.sh


##### Setup #######
#create data dir
mkdir -p $Data_DIR

## create sql load statement 
LOAD_DATA_FILE="load_data_text.sql"

rm -f ddl/$LOAD_DATA_FILE
touch ddl/$LOAD_DATA_FILE


######  Download ######	

## Flight data
for YEAR in $( seq $START $END )	
do
	wget -c http://stat-computing.org/dataexpo/2009/$YEAR.csv.bz2 -P $Data_DIR
	echo "$YEAR.csv.bz2: OK"
	sleep 1
done

echo "Flight data downloaded"

## Airport, carrier and plane data 
wget -c \
  http://stat-computing.org/dataexpo/2009/airports.csv \
  http://stat-computing.org/dataexpo/2009/carriers.csv \
  http://stat-computing.org/dataexpo/2009/plane-data.csv \
  -P $Data_DIR


echo "Carrier, airport, plane-data Dowloaded"

#Download weather data 
#wget -c https://drive.google.com/open?id=0BzgswUsgqpZSczdfbUxtdm94QU0 \ 
#https://drive.google.com/open?id=0BzgswUsgqpZSa05haEhsdEMtOUk


####### Prepare Hive table create and Data Load Statements #########
sed -i "1c create database if not exists ${DATABASE};" ddl/airline_create.sql
sed -i "2c use ${DATABASE};" ddl/airline_create.sql

 
echo "LOAD DATA INPATH '$HDFS_DIR/data/carriers.csv' INTO TABLE $DATABASE.airlines_raw;" >> ddl/$LOAD_DATA_FILE
echo "LOAD DATA INPATH '$HDFS_DIR/data/airports.csv' INTO TABLE $DATABASE.airports_raw;" >> ddl/$LOAD_DATA_FILE
echo "LOAD DATA INPATH '$HDFS_DIR/data/plane-data.csv' INTO TABLE $DATABASE.planes_raw;" >> ddl/$LOAD_DATA_FILE


for YEAR in $( seq $START $END )
do
	echo "LOAD DATA INPATH '$HDFS_DIR/data/$YEAR.csv.bz2' INTO TABLE $DATABASE.flights_raw ;" >> ddl/$LOAD_DATA_FILE
done


######  Execute ######

## Push data to hdfs 
if $(hadoop fs -test -d $HDFS_DIR ) ; 
	then sudo -u hdfs hdfs dfs -rmdir --ignore-fail-on-non-empty $HDFS_DIR
fi

hdfs dfs -mkdir -p $HDFS_DIR
hdfs dfs -copyFromLocal -f $Data_DIR/ $HDFS_DIR/
sudo -u hdfs hdfs dfs -chmod -R 777 $HDFS_DIR
sudo -u hdfs hdfs dfs -chown -R hive:hdfs $HDFS_DIR



## build jdbc URL 
export HIVE_PORT=$HIVE_PORT_BINARY
export TRANSPORT_MODE=""

if [ $HIVE_PROTOCOL == "http" ]
then 
	export TRANSPORT_MODE=";transportMode=http;httpPath=cliservice"
	if $LLAP; then export HIVE_PORT=$LLAP_PORT_HTTP; else export HIVE_PORT=$HIVE_PORT_HTTP; fi
	## must add line to change optimize.sh

else 
	if $LLAP; then export HIVE_PORT=$LLAP_PORT_BINARY; fi
fi 

export JDBC_URL="jdbc:hive2://$HIVE_HOST:$HIVE_PORT/$TRANSPORT_MODE"


### Execute hive statements 
if $OVERWRITE_TABLE; then
	echo "creating Hive structure"
	echo ""
	beeline -u $JDBC_URL -n hive -f ddl/airline_create.sql
	echo "OK"
	echo ""
fi 

echo "loading data"
echo ""
beeline -u $JDBC_URL -n hive -f ddl/$LOAD_DATA_FILE
echo ""
echo "Data Loaded"
