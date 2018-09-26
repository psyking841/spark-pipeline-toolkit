package com.pan.test.spark.batch

import com.pan.spark.batch.app.{AppParams, BatchAppSettings}
import com.pan.spark.batch.datasinks.SinkFactory
import com.pan.spark.batch.datasources.SourceFactory
import org.scalatest.{FlatSpec, Matchers}

class CommandLineTest extends FlatSpec with Matchers {
  System.setProperty("environment", "dev")
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
  System.setProperty("inputs.dataset1.layout", "hourly")

  //dataset1 specific values
  System.setProperty("inputs.dataset2.schema", "cos")
  System.setProperty("inputs.dataset2.bucket", "cos_input2_bucket")
  System.setProperty("inputs.dataset2.pathPrefix", "/cos/another/test/path")
  System.setProperty("inputs.dataset2.format", "json")
  System.setProperty("inputs.dataset2.startDate", "2018-09-03T00:00:00-0000")
  System.setProperty("inputs.dataset2.endDate", "2018-09-04T00:00:00-0000")
  System.setProperty("inputs.dataset2.layout", "daily")

  //output dataset3 specific values
  System.setProperty("outputs.dataset3.schema", "cos")
  System.setProperty("outputs.dataset3.bucket", "cos_output_bucket")
  System.setProperty("outputs.dataset3.pathPrefix", "/cos/output/test/path")
  System.setProperty("outputs.dataset3.layout", "hourly")

  val defaultSettings = new BatchAppSettings()
  val sourceFactory = new SourceFactory(defaultSettings)
  //println(sourceFactory.paramsAsJavaOptions())

  sourceFactory.paramsAsJavaOptions() should be (
    "-Dinputs.dataset1.schema=s3 -Dinputs.dataset1.bucket=input1_bucket -Dinputs.dataset1.pathPrefix=/input1/test/path " +
      "-Dinputs.dataset1.format=parquet -Dinputs.dataset1.layout=hourly -Dinputs.dataset1.startDate=2018-09-01T00:00:00-0000 " +
      "-Dinputs.dataset1.endDate=2018-09-01T01:00:00-0000 " +
      "-Dinputs.dataset2.schema=cos -Dinputs.dataset2.bucket=cos_input2_bucket -Dinputs.dataset2.pathPrefix=/cos/another/test/path " +
      "-Dinputs.dataset2.format=json -Dinputs.dataset2.layout=daily -Dinputs.dataset2.startDate=2018-09-03T00:00:00-0000 " +
      "-Dinputs.dataset2.endDate=2018-09-04T00:00:00-0000")


  val sinkFactory = new SinkFactory(defaultSettings)
  println(sinkFactory.paramsAsJavaOptions())

  sinkFactory.paramsAsJavaOptions() should be (
    "-Doutputs.dataset3.schema=cos " +
      "-Doutputs.dataset3.bucket=cos_output_bucket " +
      "-Doutputs.dataset3.pathPrefix=/cos/output/test/path " +
      "-Doutputs.dataset3.format=parquet -Doutputs.dataset3.layout=hourly " +
      "-Doutputs.dataset3.startDate=2018-09-01T00:00:00-0000"
  )

  val appParams = new AppParams(defaultSettings)
  //print(appParams.asJavaOptions())
  appParams.asJavaOptions() should be ("")
}
