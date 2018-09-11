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