package com.span.spark.batch.app

import com.typesafe.config._

/**
  * This class contains all configurations.
  *
  * The priority to take configurations (smaller index, higher priority):
  * 1. Input from command line
  * 2. Input/Output configurations of individual batch job, typically in the job config file, for example, resources/env/app.conf
  * 3. Default configurations of individual batch job, typically in the job config file, for example, resources/env/app.conf
  * 4. This library's configurations which is in the library config file resources/env/library.conf
  *
  * For example, if the option "format" appears in the three places command line, application.conf and reference.conf,
  * then the value of "format" option in command line will override all other configurations
  */
class BatchAppSettings(config: Config) {
  def this() {
    this(ConfigFactory.load())
  }

  /**
    * Get the configurations based on environment; this is a mandatory option in command line
    * I.e. -Denvironment is mandatory
    */
  protected val environment: String = config.getString("environment")

  /**
    * envConfig will be used as default Config for the application; for example, if -DawsKeyId is provided,
    * it will be available under envConfig and will be served as default value.
    * So if the specific input does not provide awsKeyId, the default awsKeyId will be used
    */
  protected val envConfig: Config =
    if(config.hasPath(environment)) config.withFallback(config.getConfig(environment))
    else config

  /**
    * sparkConfig will be used to pupolate Spark Config; for example, if -Dspark.app.name=MyApp is provided,
    * it will be used for "spark-submit ... --conf spark.app.name=MyApp".
    */
  protected val sparkConfig: Config = config.getConfig("spark_config")


  protected val hadoopConfig: Config = config.getConfig("hadoop_config")

  /**
    * Support a sets of input datasets, example of inputs:
    * First input dataset which is located in S3/IBMCOS. In your code, you can get the dataframe by
    * val input1Df = getSource("name_of_dataset1").source
    *
    * -Dinputs.name_of_dataset1.bucket=campaignevent
    * -Dinputs.name_of_dataset1.pathPrefix=path/to/data
    * -Dinputs.name_of_dataset1.layout=daily; daily/hourly, this will determine the input/output path format if no customized partition is used
    * -Dinputs.name_of_dataset1.format=json; or parquet; note, json or parquet file format indicates this is a S3/IBMCOS source
    * -Dinputs.name_of_dataset1.startDate=2018-06-01T00:00:00-0000; the format should be "yyyy-MM-ddTHH:mm:ssZ, for example 2017-12-01T11:22:33-0000
    * -Dinputs.name_of_dataset1.endDate=2018-06-01T01:00:00-0000;
    *
    */
  protected val inputs: Config = envConfig.getConfig("inputs")

  /**
    * Support a set of output datasets in S3/IBMCOS:
    * -Doutputs.name_of_dataset1.bucket=output_bucket
    * -Doutputs.name_of_dataset1.pathPrefix=path/to/rollup_data/
    * -Doutputs.name_of_dataset1.format=parquet
    * -Doutputs.name_of_dataset1.layout=daily
    * -Doutputs.name_of_dataset1.saveMode=append
    * -Doutputs.name_of_dataset1.partitionColumns=year,month,day //If this is used, the output path will not be named with startDate, instead it will be named by partition columns specified in this option
    */
  protected val outputs: Config = envConfig.getConfig("outputs")

  def inputsConfigs(inputDateName: String): Config = inputs.getConfig(inputDateName).withFallback(envConfig)
  def outputsConfigs(outputDateName: String): Config = outputs.getConfig(outputDateName).withFallback(envConfig)

  def defaultConfigs: Config = envConfig
  def sparkConfigs: Config = sparkConfig
  def hadoopConfigs: Config = hadoopConfig
}

