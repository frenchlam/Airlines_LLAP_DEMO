#!/bin/bash
set -e -x;

echo "to ORC"
hive -v -f ddl/to_orc.sql

echo "calculate stats"
hive -v -f ddl/optimize.sql
