#Overview
The exporting tool (the tool) offloads Amazon Keyspaces table to HDFS/FS without potential deadlocks and memory leaks (overhead) on the client side due to synchronous process. The tool asynchronous returns a dedicated AsyncResultSet; iteration only yields the current page,  and the next page must be fetched after the current page was persisted into HDFS/FS.
 
##The tool provides:
1. Asynchronous paging, configure the page size in the application.conf by setting basic.request.page-size=3000
2. The internal retry provides by the low level java driver, you might want to configure it by setting basic.request.default-idempotence=true and advanced.retry-policy.max-retries=6 in the application.conf
3. The postponed retry helps you to recover from failures like "StoragePartitionThroughputCapacityExceeded", "ReadThrottleEvents", or "UserErrors". Please run the tool again with same parameters and  one extra parameter --recover
4. Manual retires might be useful if you want to automate the recovering process based on your business routine. Please specify the starting page and the page rate by setting --page page-number and --rateLimiter page-per-second. The --rateLimiter might help out to avoid "StoragePartitionThroughputCapacityExceeded","ReadThrottleEvents", or "UserErrors"
 
#QuickStart
1. Before run the tool please check the capacity mode in Amazon Keyspaces Console for the table, the table should be provisioned properly (at least 3000 RCUs), or be on-demand mode
2. Configure the application.conf file, by setting:
3. the contact-point, for example, contact-points = ["cassandra.us-east-1.amazonaws.com:9142"],
the page size, the maximum page size should not exceed 12000 rows if the row size<=1KB, by default this value is 5000. Recommended value is 2500-3000  to "StoragePartitionThroughputCapacityExceeded", "ReadThrottleEvents", or "UserErrors",
    3.1. provide your login and password,
    3.2. provide trust store password and path to the jks file.  
    3.3. Run the tool in the terminal:  
    ```java -cp "AmazonKeyspacesExportTool-1.0-SNAPSHOT-fat.jar" com.amazon.aws.keyspaces.Runner "file:///Folder1/Folder2/  Target_Folder/" "select json col1, col2,...,colN from keyspace_name.table_name".```
    3.4. Please provide 2 mandatory and 2 optional parameters:
    3.5. mandatory parameter.  The target folder on HDFS/FS, for example, hdfs://target-folder/ or file://target-folder/,
    3.6. mandatory parameter.  The source query from Amazon Keyspaces, for example, “select json col1, col2,...,colN from keyspace_name.table_name”, or you can use this syntax “select json * from keyspace_name.table_name”  or “select json col1, col2,...,colN from keyspace_name.table_name where col1=value1 and col2=value2”. Keyword json must be included for all CQL queries
    3.7. optional parameter.  The starting page by default is 0, but you might want to set it to another value, for example, --page 1001. The tool will skip 1000 pages and start processing from 1001 page.
    3.8. optional parameter.  The rate limiter (page rate limiter) by default is 3000, but you might reduce it to lower value if you see "StoragePartitionThroughputCapacityExceeded" by setting --rateLimiter your-rate.

4. You can use --recover option if the tool failed with "Cassandra timeout during read query at consistency LOCAL_QUORUM (2 responses were required but only 0 replica responded)". The failed state will be persisted into state.ser file and renamed after it processed
5. Note that the tool persists each Cassandra page into a parquet file in the following format: page_number-epoch.parquet, for example, 1-1621779355501.parquet. The size of the parquet file depends on the page size.

#Validation
You can quickly validate parquet files on HDFS/FS by using Apache Spark (spark-shell), for example, “val parquetFileDF = spark.read.parquet("file:///keyspace-name/table-name"); parquetFileDF.count(); parquetFileDF.show()”
