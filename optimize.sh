#!/bin/bash
set -e -x;
export DATABASE="airline_ontime"

#Define the database 
tail -n+2 ddl/to_orc.sql > ddl/tmp_file_orc ; mv ddl/tmp_file_orc ddl/to_orc.sql
sed -i "use $DATABASE;" ../ddl/to_orc.sql

tail -n+2 ddl/optimize.sql > ddl/tmp_file_op ; mv ddl/tmp_file_opt ddl/optimize.sql
sed -i "use $DATABASE;" ../ddl/optimize.sql

#execute the scripts
echo "to ORC"
hive -v -f ddl/to_orc.sql

echo "calculate stats"
hive -v -f ddl/optimize.sql
