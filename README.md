# SDS Endterm Final Project

## Overleaf Link:

https://www.overleaf.com/4524613188dnwkxyqpjkpd#fb938f

## Generation:

Navigate to the generation folder in the project. The generate.py script accepts three arguments: throughput, total duration and max span.
The code has also been modified to insert into PostgresSQL.

Before running the spark code, also run the id_col.py and id_col_bisignal.py files.

To run sanity checks on the data and for result verification (after running Spark or Flink), equivalent SQL scripts are available in the sql_scripts directory


## Spark:

Navigate to processing_spark for the first query, and processing_spark_2 for the second query. Navigate to the directory where spark is installed in the local system and run:

./bin/spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 project_directory/processing_spark/mono/rate_streaming_2.py 
