#!/bin/bash
set -e -x;

### Uncomment all pertinent values

### Data dirs
export Data_DIR="./data"
export HDFS_DIR="/tmp/airline_demo"

### scope of the flights data - Year
### Possible values between 1988 and 2008
export START="1988"
export END="2008"

## Hive connection Configurations
export HIVE_HOST="localhost"
export DATABASE="airline_ontime"

export HIVE_PROTOCOL="http"  # binary | http
export LLAP=false #wether or not to use LLAP

## defaults ports
export HIVE_PORT_BINARY=10000
export LLAP_PORT_BINARY=10500
export HIVE_PORT_HTTP=10001
export LLAP_PORT_HTTP=10501
