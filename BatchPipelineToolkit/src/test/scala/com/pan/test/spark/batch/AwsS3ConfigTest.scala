package com.pan.test.spark.batch

import com.pan.spark.batch.app.BatchAppSettings
import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.Config

class AwsS3ConfigTest extends FlatSpec with Matchers {

  "Settings for AWS S3 inputs/outputs" should "be properly taken" in {
    //default values
    System.setProperty("environment", "local")
    System.setProperty("schema", "s3")
    System.setProperty("awsKeyId", "ABCDEFG")
    System.setProperty("awsSecretKey", "12345678!@#$%")
    System.setProperty("format", "parquet")
    System.setProperty("startDate", "2018-06-01T00:01:00-0000") //default start date
    System.setProperty("endDate", "2018-06-01T00:02:00-0000") //default end date

    //Assuming we have two input data sources: dataset1 and dataset2
    //dataset1 specific values
    System.setProperty("inputs.dataset1.bucket", "input1_bucket")
    System.setProperty("inputs.dataset1.pathPrefix", "/input1/test/path")
    System.setProperty("inputs.dataset1.startDate", "input_sd1")
    System.setProperty("inputs.dataset1.layout", "hourly")

    //dataset2 specific values
    System.setProperty("inputs.dataset2.bucket", "input2_bucket")
    System.setProperty("inputs.dataset2.pathPrefix", "/another/test/path")
    System.setProperty("inputs.dataset2.format", "json")
    System.setProperty("inputs.dataset2.layout", "hourly")

    //Assuming we have one data sink: dataset3
    //output dataset3 specific values
    System.setProperty("outputs.dataset3.bucket", "output_bucket")
    System.setProperty("outputs.dataset3.pathPrefix", "/output/test/path")
    System.setProperty("outputs.dataset3.layout", "hourly")

    val settings = new BatchAppSettings()
    val defaultConfig: Config = settings.getDefaultConfigs
    val input1: Config = settings.getInputsConfigs("dataset1")
    val input2: Config = settings.getInputsConfigs("dataset2")

    val output: Config = settings.getOutputsConfigs("dataset3")

    //with fallback we can get startDate for output3 even though we did not specify it for output3
    output.withFallback(defaultConfig).getString("startDate") should be ("2018-06-01T00:01:00-0000")

    //eval default values
    defaultConfig.getString("schema") should be ("s3")
    defaultConfig.getString("environment") should be ("local")
    defaultConfig.getString("awsKeyId") should be ("ABCDEFG")
    defaultConfig.getString("awsSecretKey") should be ("12345678!@#$%")
    defaultConfig.getString("startDate") should be ("2018-06-01T00:01:00-0000")
    defaultConfig.getString("endDate") should be ("2018-06-01T00:02:00-0000")
    defaultConfig.getString("environment") should be ("local")

    //eval input sepcific values
    input1.getString("bucket") should be ("input1_bucket")
    input1.getString("pathPrefix") should be ("/input1/test/path")
    input1.getString("format") should be ("parquet") //should take the default value
    input1.getString("startDate") should be ("input_sd1") //should override the default
    input1.getString("layout") should be ("hourly") //should override the default

    //if key not available for specific input, take that from default
    input1.getString("awsKeyId") should be ("ABCDEFG")
    input1.getString("endDate") should be ("2018-06-01T00:02:00-0000")

    input2.getString("bucket") should be ("input2_bucket")
    input2.getString("pathPrefix") should be ("/another/test/path")
    input2.getString("format") should be ("json")
    input2.getString("startDate") should be ("2018-06-01T00:01:00-0000") //should take the default value

    //evalu output specific values
    output.getString("bucket") should be ("output_bucket")
    output.getString("pathPrefix") should be ("/output/test/path")
    output.getString("startDate") should be ("2018-06-01T00:01:00-0000")
  }
}
