
LOAD_DATA_FILE="load_data_text.sql"

cd ~/Airlines_LLAP_DEMO-master
rm -f ./ddl/$LOAD_DATA_FILE

touch ./ddl/$LOAD_DATA_FILE

for YEAR in {2007..2008}
do
	echo "LOAD DATA LOCAL INPATH '$(pwd)/data/$YEAR.csv.bz2' INTO TABLE airline_ontime.flights_raw ;" >> ./ddl/$LOAD_DATA_FILE
done

#beeline -u jdbc:hive2://localhost:10000/ -n hive -f ddl/$LOAD_DATA_FILE
