# Hive Airline on_time Dataset

Creates Hive database based on the Airline On_time dataset ([http://stat-computing.org/dataexpo/2009/](http://stat-computing.org/dataexpo/2009/)) that can be explored using a Tableau Dashboard. 

# Usage 

#### Requierments : 
The scripts should be run from a machine :
- with access to the Hadoop cluster and 
- having HDFS and HIVE (beeline) clients installed and configured

### 1. Setup - Update the configuration file: "config.sh"
By default the script will:
- Download the entire dataset (from 1998 to 2008).
  To limit the scope, please adjust the *START* and *END* parameters 
  
- Use *localhost* to connect to the Hive Server
  Please adjust the *HIVE_HOST* parameter with the location of Hive Server 2 
  
- Use Tez as the Default execution engine. 
  To use LLAP (recommended) please adjust the *LLAP* parameter to *true*
  
  
### 2. Run the scripts 
Start by executing *download.sh*.
It will download the Data from the *stat-computing.org* site and create a staging table on top of the data
```bash
./download.sh
````

Then run *optimize.sh*.
It will will create an optimized partitionned Hive table using ORC
```bash
./optimize.sh
```

### Optional - Druid integration
A 3rd script is available to create a denormalized table backed by Druid : *druid_fact_table.sh*
**NOTE:** Druid must be proporly installed and configured ( including Hive integration ) before running this script. 
```bash 
./druid_fact_table.sh
```


