package com.pan.test.spark.batch

import com.pan.spark.batch.app.BatchAppSettings
import com.typesafe.config.Config
import org.scalatest.{FlatSpec, Matchers}

class IbmCosConfigTest extends FlatSpec with Matchers {
  "Settings for IBM COS inputs/outputs" should "be properly taken" in {
    //default values
    System.setProperty("environment", "local")
    System.setProperty("schema", "cos")
    System.setProperty("ibmServiceName", "myCos")
    System.setProperty("endpoint", "https://xyz.softlayer.net")
    System.setProperty("serviceId", "ServiceId-12345")
    System.setProperty("ibmCosAccessKey", "12345678!@#$%")
    System.setProperty("ibmCosSecretKey", "abcde")
    System.setProperty("format", "parquet")
    System.setProperty("startDate", "2018-09-01T00:00:00-0000") //default start date
    System.setProperty("endDate", "2018-09-02T00:00:00-0000") //default end date
    System.setProperty("layout", "daily")

    //Assuming we have two input data sources: cos_dataset1
    //dataset1 specific values
    System.setProperty("inputs.cos_dataset1.bucket", "cos_input1_bucket")
    System.setProperty("inputs.cos_dataset1.pathPrefix", "/cos/input1/test/path")
    System.setProperty("inputs.cos_dataset1.startDate", "cos_input_sd1")

    //Assuming we have one data sink: cos_dataset2
    //output cos_dataset2 specific values
    System.setProperty("outputs.cos_dataset2.bucket", "cos_output_bucket")
    System.setProperty("outputs.cos_dataset2.pathPrefix", "/cos/output/test/path")

    val ibm_cos_settings = new BatchAppSettings()
    val ibmCosDefaultConfig: Config = ibm_cos_settings.getDefaultConfigs

    val input1: Config = ibm_cos_settings.getInputsConfigs("cos_dataset1")
    val output: Config = ibm_cos_settings.getOutputsConfigs("cos_dataset2")

    //with fallback we can get startDate for output3 even though we did not specify it for output3
    output.withFallback(ibmCosDefaultConfig).getString("startDate") should be ("2018-09-01T00:00:00-0000")

    //eval default values
    ibmCosDefaultConfig.getString("schema") should be ("cos")
    ibmCosDefaultConfig.getString("environment") should be ("local")
    ibmCosDefaultConfig.getString("ibmServiceName") should be ("myCos")
    ibmCosDefaultConfig.getString("endpoint") should be ("https://xyz.softlayer.net")
    ibmCosDefaultConfig.getString("serviceId") should be ("ServiceId-12345")
    ibmCosDefaultConfig.getString("ibmCosAccessKey") should be ("12345678!@#$%")
    ibmCosDefaultConfig.getString("ibmCosSecretKey") should be ("abcde")
    ibmCosDefaultConfig.getString("startDate") should be ("2018-09-01T00:00:00-0000")
    ibmCosDefaultConfig.getString("endDate") should be ("2018-09-02T00:00:00-0000")
    ibmCosDefaultConfig.getString("environment") should be ("local")

    //eval input sepcific values
    input1.getString("bucket") should be ("cos_input1_bucket")
    input1.getString("pathPrefix") should be ("/cos/input1/test/path")
    input1.getString("format") should be ("parquet") //should take the default value
    input1.getString("startDate") should be ("cos_input_sd1") //should override the default

    //if key not available for specific input, take that from default
    input1.getString("ibmCosAccessKey") should be ("12345678!@#$%")
    input1.getString("endDate") should be ("2018-09-02T00:00:00-0000")
    input1.getString("layout") should be ("daily")

    //evalu output specific values
    output.getString("bucket") should be ("cos_output_bucket")
    output.getString("pathPrefix") should be ("/cos/output/test/path")
    output.getString("startDate") should be ("2018-09-01T00:00:00-0000")
    output.getString("layout") should be ("daily")
  }
}
