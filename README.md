# Spark Pipeline Toolkit library

Author: Shengyi Pan

## Problem Statement
When building a batch job pipeline, one might run into the trouble in generating a spark-submit command line for the frequent-run Spark job. For example, for a daily-run Spark job, the date string in the spark-submit command line needs to be changed daily/hourly (depending on how frequent your batch job runs).

One might also run into the issue where he/she has to read data from different data sources, so the application code could become messy because configurations for different sources have to be setup in the applicaiton code together with logic.

## Solutions Provided by This Project
This is one configuration file each Spark job so that all configurations should go there. These include the dates on which this job should run (however, dates should be modifiable through spark-submit command line), as well as input/output datasets locations, and all others necessary.
The Spark job should only contain the data transformation logic (or machine learning algorithm, see [WordCountDemo](./WordCountDemo)). When developing Spark job log and the logic, the machine learning engineers or data scientists should not worry about the configurations, such as in what media the data is stored (however, they should know at least they need "hello world" table in AWS S3). Such configurations should be able to look up from a dataset metastore (a future work to this project).

This project also provides an interface (through command line arguments) to schedulers like Airflow, so that dynamic argumets like job start date and/or end date can be passed on fly. Again, the developers do not need to know how the dates are passed in, they just need to know their job is running daily/hourly/weekly at, for example 1 am of the day. And all these can be configured in the configuration file once before starting developing the Spark job.

For example see 'Explanation for Demo Code'

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

Note that above command line will use the log4j property file from Spark library, so if you just simply run ```java -DdryRun -jar WordCountDemo.jar``` it won't print anything until you have a log4j.properties file in your classpath

This will print the full command line:
```
spark-submit master=local[*] -conf spark.submit.deployMode=client -conf spark.sql.session.timeZone=UTC --driver-java-options '-Denvironment=dev -DstartDate=2018-10-09T02:16:38+0000 -DendDate=2018-10-09T02:16:38+0000 -Dfs.s3a.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem -Dfs.cos.ibmServiceName.access.key=changeme -Dfs.cos.ibmServiceName.endpoint=changeme -Dfs.stocator.scheme.list=cos -Dfs.cos.ibmServiceName.iam.service.id=changeme -Dfs.s3a.awsSecretAccessKey=changeme -Dfs.cos.ibmServiceName.v2.signer.type=false -Dfs.s3a.awsAccessKeyId=changeme -Dfs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem -Dfs.cos.ibmServiceName.secret.key=changeme -Dfs.stocator.cos.scheme=cos -Dfs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient -Dinputs.textData.schema=cos -Dinputs.textData.bucket=test-bucket-span001 -Dinputs.textData.pathPrefix=/source -Dinputs.textData.format=textFile -Dinputs.textData.layout=daily -Dinputs.textData.startDate=changeme -Dinputs.textData.endDate=changeme -Doutputs.wordCountData.schema=cos -Doutputs.wordCountData.bucket=test-bucket-span001 -Doutputs.wordCountData.pathPrefix=/word_count_sink -Doutputs.wordCountData.format=parquet -Doutputs.wordCountData.layout=customized -Doutputs.wordCountData.startDate=changeme' --class [class name] [jar location]
```

## Explanation for Demo Code
The [app.conf](./WordCountDemo/src/main/resources/dev/app.conf) file contains all the configurations related to this job.
It tells the job where to lookup the input data and where to store the output data. These should usually be specified once before developing the job.
In the examle the input data is named as "textData", and output data is named as "wordCountData". Also note that it allows multiple input/output datasets, you just need to name them so you can refer them in the code.


In the job, one just need to use

    val cosTextFile: DataFrame = getDataSourceFor("textData").source
    
to read the text data which is location in IBM COS (as specified in app.conf by schema field under textData block).

One also just need to use 

    getDataSinkFor("wordCountData").emit(wordCountsDf)

to output the results to S3 (as specified in app.conf by schema filed under wordCountData block).