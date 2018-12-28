#!/bin/bash
set -e -x;

export Data_DIR="./data"
export HDFS_DIR="/tmp/airline_demo"

export START="1988"
export END="2008"

export DATABASE="airline_ontime"
export HIVE_PROTOCOL="binary"  # binary | http
export LLAP=false
export HIVE_PORT=10000
export HIVE_HOST="localhost"

export OVERWRITE_TABLE=true


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

for each in *.csv
do 
	rm $each.gz || true 
	gzip -1 $each
done

echo "Carrier, airport, plane-data Dowloaded"

#Download weather data 
#wget -c https://drive.google.com/open?id=0BzgswUsgqpZSczdfbUxtdm94QU0 \ 
#https://drive.google.com/open?id=0BzgswUsgqpZSa05haEhsdEMtOUk


####### Prepare Hive table create and Data Load Statements #########
##create table structure
if $OVERWRITE_TABLE; then
	sed -i "1s/^/DROP TABLE IF EXISTS flights_raw PURGE;/" ddl/airline_create.sql
	sed -i '1i\\' ddl/airline_create.sql
fi

sed -i "1s/^/use ${DATABASE};/" ddl/airline_create.sql
sed -i '1i\\' ddl/airline_create.sql
sed -i "1s/^/create database if not exists ${DATABASE};/" ddl/airline_create.sql

sed -i "1s/^/use ${DATABASE};/" ddl/airline_create.sql

 
echo "LOAD DATA INPATH '$HDFS_DIR/data/carriers.csv.gz' INTO TABLE $DATABASE.airlines_raw;" >> ddl/$LOAD_DATA_FILE
echo "LOAD DATA INPATH '$HDFS_DIR/data/airports.csv.gz' INTO TABLE $DATABASE.airports_raw;" >> ddl/$LOAD_DATA_FILE
echo "LOAD DATA INPATH '$HDFS_DIR/data/plane-data.csv.gz' INTO TABLE $DATABASE.planes_raw;" >> ddl/$LOAD_DATA_FILE


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
if [ $HIVE_PROTOCOL == "http" ]
then 
	export TRANSPORT_MODE=";transportMode=http;httpPath=cliservice"
	if $LLAP; then export PORT=10500; else export PORT=10001; fi
	## must add line to change optimize.sh

else 
	export TRANSPORT_MODE=""
	if $LLAP; then export PORT=10500; fi
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
echo "OK"


# create structure
#beeline -u jdbc:hive2://localhost:10000/ -n hive -f ../ddl/airline_create.sql
#echo "structure created"

#load data 
#echo "loading data"
#beeline -u jdbc:hive2://localhost:10000/ -n hive -f ../ddl/$LOAD_DATA_FILE

