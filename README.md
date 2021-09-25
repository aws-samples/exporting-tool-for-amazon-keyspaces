# Exporting Tool for Amazon Keyspaces
The exporting tool offloads the Amazon Keyspaces table to HDFS/FS
 
# Build this project
To build and use this library, execute the following mvn command. 
```
mvn install package
```

# Quick start
Before running the tool, verify the capacity mode of the source table. The table should be provisioned with at least 3,000 RCUs, 
or be configured for on-demand mode. The recommendation is to set the page size for the driver in the application.conf file to 2,500.
Run the tool in the terminal with the following command:
 
`java -cp "AmazonKeyspacesExportTool-1.0-SNAPSHOT-fat.jar" com.amazon.aws.keyspaces.Runner HDFS_FOLDER SOURCE_QUERY [--recover]`
 
Please set the following required parameters:
`HDFS_FOLDER` – The target folder on HDFS/FS. For example, `hdfs://target-folder/` or `file://target-folder/`
`SOURCE_QUERY` – The source query from Amazon Keyspaces. JSON Keyword must be included. For example, 
`select json col1, col2,...,colN from keyspace_name.table_name`, or you can use this syntax `select json * from keyspace_name.table_name`, or 
`select json col1, col2,...,colN from keyspace_name.table_name where col1=value1 and col2=value2`
 
If you need to re-start the process, you can use the optional recover option to resume from where the tool left off.
 
RECOVER OPTION – you can use the `--recover` option if the tool failed with 
`Cassandra timeout during read query at consistency LOCAL_QUORUM (2 responses were required but only 0 replica responded)`. 
The failed state will be saved in a state.ser file and renamed after it is processed.
 
# Validation
You can validate the parquet files on HDFS/FS by using Apache Spark (spark-shell). 
For example, 
```
    val parquetFileDF = spark.read.parquet("file:///keyspace-name/table-name") 
    parquetFileDF.count() 
    parquetFileDF.show()
```
 # License
 
 This tool is licensed under the Apache-2 License. See the LICENSE file.