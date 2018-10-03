# Spark Pipeline Toolkit library

Author: Shengyi Pan

## Problem Statement
When building a batch job pipeline, one might run into the trouble in generating a spark-submit command for the frequent-run Spark job. For example, for a daily-run Spark job, the date need to be changed every day.

One might also run into the issue where he/she has to read data from different data sources, so the application code could become messy because configurations for different sources have to be setup in the applicaiton code together with logic.

## Features
### Spark command line auto generation

### Modulization of configurations and application code (the code for your Spark application logic)

### Support for IBMCOS, S3 as source and sink
Will support AWS redshift and IBM DB2 Warehouse

## Usage
See WordCountDemo project for how to use.
There is a "dry-run" mode to just print the full spark-submit command line that is going to be run.
To enable the "dry-run" mode, have -DdryRun passed to java options.
For example:
```
spark-submit --driver-java-options "-DdryRun" --class com.span.spark.apps.wordcountdemo.WordCountDemo /Users/shengyipan/Workspace/spark-pipeline-toolkit/WordCountDemo/target/scala-2.11/WordCountDemo.jar
```

This will print the full command line:
```
spark-submit master=local[*] -conf spark.jars=file:/Users/shengyipan/Workspace/spark-pipeline-toolkit/WordCountDemo/target/scala-2.11/WordCountDemo.jar -conf spark.app.name=com.span.spark.apps.wordcountdemo.WordCountDemo -conf spark.sql.session.timeZone=UTC -conf spark.driver.extraJavaOptions=-DdryRun -DstartDate=2018-10-01T00:00:00-0000 -DendDate=2018-10-01T01:00:00-0000 -conf spark.submit.deployMode=client --driver-java-options '-Denvironment=dev -DstartDate=2018-10-01T00:00:00-0000 -DendDate=2018-10-01T01:00:00-0000 -Dfs.stocator.scheme.list=cos -Dfs.s3a.awsSecretAccessKey=changeme -Dfs.cos.ibmServiceName.endpoint=changeme -Dfs.cos.ibmServiceName.v2.signer.type=false -Dfs.cos.ibmServiceName.access.key=changeme -Dfs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient -Dfs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem -Dfs.s3a.awsAccessKeyId=changeme -Dfs.cos.ibmServiceName.secret.key=changeme -Dfs.stocator.cos.scheme=cos -Dfs.s3a.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem -Dfs.cos.ibmServiceName.iam.service.id=changeme -Dinputs.textData.schema=cos -Dinputs.textData.bucket=test-bucket-span001 -Dinputs.textData.pathPrefix=/source -Dinputs.textData.format=textFile -Dinputs.textData.layout=daily -Dinputs.textData.startDate=2018-10-01T00:00:00-0000 -Dinputs.textData.endDate=2018-10-01T01:00:00-0000 -Doutputs.wordCountData.schema=cos -Doutputs.wordCountData.bucket=test-bucket-span001 -Doutputs.wordCountData.pathPrefix=/word_count_sink -Doutputs.wordCountData.format=parquet -Doutputs.wordCountData.layout=customized -Doutputs.wordCountData.startDate=2018-10-01T00:00:00-0000' --class [class name] [jar location]
```
