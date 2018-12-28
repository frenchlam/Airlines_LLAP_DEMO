#!/bin/bash
set -e -x;

## Default values 
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


## build jdbc URL 
export HIVE_PORT=$HIVE_PORT_BINARY
export TRANSPORT_MODE=""

if [ $HIVE_PROTOCOL == "http" ]
then 
	export TRANSPORT_MODE=";transportMode=http;httpPath=cliservice"
	if $LLAP; then export HIVE_PORT=$LLAP_PORT_HTTP; else export HIVE_PORT=$HIVE_PORT_HTTP; fi
else 
	if $LLAP; then export HIVE_PORT=$LLAP_PORT_BINARY; fi
fi 

export JDBC_URL="jdbc:hive2://$HIVE_HOST:$HIVE_PORT/$TRANSPORT_MODE"


#Define the database 
#tail -n+2 ddl/to_orc.sql > ddl/tmp_file_orc ; mv ddl/tmp_file_orc ddl/to_orc.sql
sed -i "1c use ${DATABASE};" ddl_druid/to_druid.sql


#execute the scripts
echo "to ORC"
beeline -u $JDBC_URL -n hive -f ddl/to_druid.sql


