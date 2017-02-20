#!/bin/bash
set -e -x;

Data_DIR="Airlines_LLAP_DEMO-master/data"
export START="2007"
export END="2008"


mkdir -p $Data_DIR
cd $Data_DIR

for YEAR in {$START..$END}
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
hive -v -f ddl/airline_create.sql
echo "structure created"

cd ..
