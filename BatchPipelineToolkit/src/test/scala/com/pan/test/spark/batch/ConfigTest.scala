package com.pan.test.spark.batch

import com.pan.spark.batch.app.BatchAppSettings
import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.Config

class ConfigTest extends FlatSpec with Matchers {

  "Settings for inputs/outputs" should "be properly taken" in {

    System.setProperty("environment", "local")
    System.setProperty("format", "parquet")
    System.setProperty("startDate", "2018-09-01T00:00:00-0000") //default start date
    System.setProperty("endDate", "2018-09-01T01:00:00-0000") //default end date

    //Default configs for reading from/writing to S3
    System.setProperty("awsKeyId", "ABCDEFG")
    System.setProperty("awsSecretKey", "12345678!@#$%")

    //Default configs for reading from/writing to IBM COS
    System.setProperty("ibmServiceName", "myCos")
    System.setProperty("endpoint", "https://xyz.softlayer.net")
    System.setProperty("serviceId", "ServiceId-12345")
    System.setProperty("ibmCosAccessKey", "87654321!@#$%")
    System.setProperty("ibmCosSecretKey", "abcde")

    //Assuming we have two input data sources: dataset1 at S3 and dataset2 at IBM COS
    //dataset1 specific values
    System.setProperty("inputs.dataset1.schema", "s3")
    System.setProperty("inputs.dataset1.bucket", "input1_bucket")
    System.setProperty("inputs.dataset1.pathPrefix", "/input1/test/path")
    System.setProperty("inputs.dataset1.startDate", "input_sd1")
    System.setProperty("inputs.dataset1.layout", "hourly")

    //dataset2 specific values
    System.setProperty("inputs.dataset2.schema", "cos")
    System.setProperty("inputs.dataset2.bucket", "cos_input2_bucket")
    System.setProperty("inputs.dataset2.pathPrefix", "/cos/another/test/path")
    System.setProperty("inputs.dataset2.format", "json")
    System.setProperty("inputs.dataset2.startDate", "2018-09-03T00:00:00-0000")
    System.setProperty("inputs.dataset2.endDate", "2018-09-04T00:00:00-0000")
    System.setProperty("inputs.dataset2.layout", "daily")

    //Assuming we have one data sink: dataset3 at IBM COS
    //output dataset3 specific values
    System.setProperty("outputs.dataset3.schema", "cos")
    System.setProperty("outputs.dataset3.bucket", "cos_output_bucket")
    System.setProperty("outputs.dataset3.pathPrefix", "/cos/output/test/path")
    System.setProperty("outputs.dataset3.layout", "hourly")

    val settings = new BatchAppSettings()
    val defaultConfig: Config = settings.getDefaultConfigs
    val input1: Config = settings.getInputsConfigs("dataset1")
    val input2: Config = settings.getInputsConfigs("dataset2")

    val output: Config = settings.getOutputsConfigs("dataset3")

    //with fallback we can get startDate for output3 even though we did not specify it for output3
    output.withFallback(defaultConfig).getString("startDate") should be ("2018-09-01T00:00:00-0000")

    //eval default values for s3
    defaultConfig.getString("environment") should be ("local")
    defaultConfig.getString("awsKeyId") should be ("ABCDEFG")
    defaultConfig.getString("awsSecretKey") should be ("12345678!@#$%")
    defaultConfig.getString("startDate") should be ("2018-09-01T00:00:00-0000")
    defaultConfig.getString("endDate") should be ("2018-09-01T01:00:00-0000")

    //eval default values for IBM COS
    defaultConfig.getString("ibmServiceName") should be ("myCos")
    defaultConfig.getString("endpoint") should be ("https://xyz.softlayer.net")
    defaultConfig.getString("serviceId") should be ("ServiceId-12345")
    defaultConfig.getString("ibmCosAccessKey") should be ("87654321!@#$%")
    defaultConfig.getString("ibmCosSecretKey") should be ("abcde")

    //eval input dataset1 sepcific values
    input1.getString("bucket") should be ("input1_bucket")
    input1.getString("pathPrefix") should be ("/input1/test/path")
    input1.getString("schema") should be ("s3")
    input1.getString("format") should be ("parquet") //should take the default value
    input1.getString("startDate") should be ("input_sd1") //should override the default
    input1.getString("layout") should be ("hourly") //should override the default

    //if key not available for specific input, inherit that from default
    input1.getString("awsKeyId") should be ("ABCDEFG")
    input1.getString("endDate") should be ("2018-09-01T01:00:00-0000")

    //eval input dataset1 sepcific values
    input2.getString("bucket") should be ("cos_input2_bucket")
    input2.getString("pathPrefix") should be ("/cos/another/test/path")
    input2.getString("schema") should be ("cos")
    input2.getString("format") should be ("json")
    input2.getString("startDate") should be ("2018-09-03T00:00:00-0000") //should override the default startDate value
    input2.getString("endDate") should be ("2018-09-04T00:00:00-0000") //should override the default startDate value

    //evalu output specific values
    output.getString("bucket") should be ("cos_output_bucket")
    output.getString("pathPrefix") should be ("/cos/output/test/path")
    output.getString("startDate") should be ("2018-09-01T00:00:00-0000")
  }
}
