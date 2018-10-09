package com.span.test.spark.batch

import com.span.spark.batch.app.{AppParams, BatchAppSettings}
import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.Config

class ConfigTest extends FlatSpec with Matchers {

  "Settings for inputs/outputs" should "be properly taken" in {

    System.setProperty("environment", "dev")
    System.setProperty("format", "parquet")
    System.setProperty("startDate", "2018-09-01T00:00:00-0000") //default start date
    System.setProperty("endDate", "2018-09-01T01:00:00-0000") //default end date

    //Adding two additional input data sources: dataset1 at S3 and dataset2 at IBM COS to those in test/resources
    //dataset1 specific values
    System.setProperty("inputs.dataset1.schema", "s3")
    System.setProperty("inputs.dataset1.bucket", "input1_bucket")
    System.setProperty("inputs.dataset1.pathPrefix", "/input1/test/path")
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

    //Test cases for spark conf, it will take those in reference.conf plus "spark.app.name=MyApp" as below
    System.setProperty("spark_config.spark.app.name", "MyApp")

    //Test fetching configurations for spark options
    val settings = new BatchAppSettings()
    val appParams = new AppParams(settings)

    appParams.sparkSessionOptionsMap should be (
      Map("master" -> "local[*]",
        "spark.app.name" -> "MyApp",
        "spark.submit.deployMode" -> "client",
        "spark.sql.session.timeZone" -> "UTC")
    )

    appParams.hadoopOptionsMap should be (
      Map("fs.s3a.impl" -> "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
        "fs.cos.ibmServiceName.access.key" -> "changeme",
        "fs.cos.ibmServiceName.endpoint" -> "changeme",
        "fs.stocator.scheme.list" -> "cos",
        "fs.cos.ibmServiceName.iam.service.id" -> "changeme",
        "fs.s3a.awsSecretAccessKey" -> "changeme",
        "fs.cos.ibmServiceName.v2.signer.type" -> "false",
        "fs.s3a.awsAccessKeyId" -> "changeme",
        "fs.cos.impl" -> "com.ibm.stocator.fs.ObjectStoreFileSystem",
        "fs.cos.ibmServiceName.secret.key" -> "changeme",
        "fs.stocator.cos.scheme" -> "cos",
        "fs.stocator.cos.impl" -> "com.ibm.stocator.fs.cos.COSAPIClient")
    )

    //Test fetching configurations for the app
    val defaultConfig: Config = settings.defaultConfigs
    val input1: Config = settings.inputsConfigs("dataset1")
    val input2: Config = settings.inputsConfigs("dataset2")
    val textData: Config = settings.inputsConfigs("textData") //from test/resources
    val output: Config = settings.outputsConfigs("dataset3")

    //with fallback we can get startDate for output3 even though we did not specify it for output3
    output.withFallback(defaultConfig).getString("startDate") should be ("2018-09-01T00:00:00-0000")

    //eval default values
    defaultConfig.getString("environment") should be ("dev")
    defaultConfig.getString("startDate") should be ("2018-09-01T00:00:00-0000")
    defaultConfig.getString("endDate") should be ("2018-09-01T01:00:00-0000")

    //eval input dataset configs from test/resources
    textData.getString("bucket") should be ("test-bucket-span001")
    textData.getString("pathPrefix") should be ("/source")
    textData.getString("schema") should be ("cos")
    textData.getString("format") should be ("textFile") //should take the default value
    textData.getString("startDate") should be ("2018-09-01T00:00:00-0000") //should take the default
    textData.getString("layout") should be ("daily") //should override the default

    //eval input dataset1 sepcific values
    input1.getString("bucket") should be ("input1_bucket")
    input1.getString("pathPrefix") should be ("/input1/test/path")
    input1.getString("schema") should be ("s3")
    input1.getString("format") should be ("parquet") //should take the default value
    input1.getString("startDate") should be ("2018-09-01T00:00:00-0000") //should take the default
    input1.getString("layout") should be ("hourly") //should override the default

    //if key not available for specific input, inherit that from default
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
