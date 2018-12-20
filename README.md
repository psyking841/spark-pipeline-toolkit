# Spark Pipeline Toolkit library

Creator: Shengyi Pan

Please feel free to file pull requests.

## What this library does?
Using this library allows you to save your configurations to a Spark job in a json file. And the library provides logic to parse this configurations. I mean to make this library much easier to add new configurations and new parsers.

## Problem Statement
When building a batch job pipeline, one might run into the trouble in generating a spark-submit command line for the frequent-run Spark job. For example, for a daily-run Spark job, the date string in the spark-submit command line needs to be changed daily/hourly (depending on how frequent your batch job runs).

Or, when you are running Spark job with a scheduler, say Airflow, you typically store parameters (for your Spark job to communicate to DB or Microservices) in DAG file. The downside of doing this is that you are basically using Airflow as a storage for parameters (isn't this what DB should do for you?), which makes your DAG script bloated. It would be nice if the configurations is stored in a DB friendly format like json, yaml etc.

To my point of view, the parameters should be stored with the Spark job itself, as different Spark job will need different sets of parameters. When another developer is reading your code, parameters should be within the reach to increase the readability (for example, what configurations and default parameters to the configurations this job is using).  However, when you put your configuration parsing logic in the code (I mean the Spark application code), the code could easily become bloated because you will have to include all the parsing logic. For example, one might also run into the issue where he/she has to read data from different data sources (e.g. different DBs, different Cloud Storage services), configurations to connect to these data sources have to be setup in the applicaiton code together with the parsing logic.

Therefore, there is a need for a place/library to have all those parsing logic for the configurations/parameters in json or yaml.

## Features Description
### Spark command line auto generation
Descriptions. (TO DO)

### Modulization of configurations and application code (the code for your Spark application logic)
Descriptions. (TO DO)

### Support for IBM COS, S3 as source and sink
Descriptions for IBM COS. (TO DO)
Will support AWS redshift and IBM DB2 Warehouse.

## Usage
See WordCountDemo project for how to use.
There is a "dry-run" mode to just print the full spark-submit command line that is going to be run.
To enable the "dry-run" mode, have -DdryRun passed to java options.
For example:
```
spark-submit --driver-java-options "-DdryRun" --class com.span.spark.apps.wordcountdemo.WordCountDemo /Users/shengyipan/Workspace/spark-pipeline-toolkit/WordCountDemo/target/scala-2.11/WordCountDemo.jar
```

Note that above command line will use the log4j property file from Spark library, so if you just simply run ```java -DdryRun -jar WordCountDemo.jar``` it won't print anything until you have a log4j.properties file in your classpath

This will print the full command line:
```
spark-submit master=local[*] -conf spark.submit.deployMode=client -conf spark.sql.session.timeZone=UTC --driver-java-options '-Denvironment=dev -DstartDate=2018-10-09T02:16:38+0000 -DendDate=2018-10-09T02:16:38+0000 -Dfs.s3a.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem -Dfs.cos.ibmServiceName.access.key=changeme -Dfs.cos.ibmServiceName.endpoint=changeme -Dfs.stocator.scheme.list=cos -Dfs.cos.ibmServiceName.iam.service.id=changeme -Dfs.s3a.awsSecretAccessKey=changeme -Dfs.cos.ibmServiceName.v2.signer.type=false -Dfs.s3a.awsAccessKeyId=changeme -Dfs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem -Dfs.cos.ibmServiceName.secret.key=changeme -Dfs.stocator.cos.scheme=cos -Dfs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient -Dinputs.textData.schema=cos -Dinputs.textData.bucket=test-bucket-span001 -Dinputs.textData.pathPrefix=/source -Dinputs.textData.format=textFile -Dinputs.textData.layout=daily -Dinputs.textData.startDate=changeme -Dinputs.textData.endDate=changeme -Doutputs.wordCountData.schema=cos -Doutputs.wordCountData.bucket=test-bucket-span001 -Doutputs.wordCountData.pathPrefix=/word_count_sink -Doutputs.wordCountData.format=parquet -Doutputs.wordCountData.layout=customized -Doutputs.wordCountData.startDate=changeme' --class [class name] [jar location]
```
