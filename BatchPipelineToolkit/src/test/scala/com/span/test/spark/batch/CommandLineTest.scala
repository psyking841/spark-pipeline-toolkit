package com.span.test.spark.batch

import com.span.spark.batch.app.{AppParams, BatchAppBase, BatchAppSettings}
import com.span.spark.batch.datasinks.SinkFactory
import com.span.spark.batch.datasources.SourceFactory

import org.scalatest.{FlatSpec, Matchers}

class CommandLineTest extends FlatSpec with Matchers {
  System.setProperty("environment", "dev")
  System.setProperty("dryRun", "") //Dryrun mode - only print the spark-submit command line
  System.setProperty("format", "parquet")
  System.setProperty("startDate", "2018-09-01T00:00:00-0000") //default start date
  System.setProperty("endDate", "2018-09-01T01:00:00-0000") //default end date

  val defaultSettings = new BatchAppSettings()
  val sourceFactory = new SourceFactory(defaultSettings)

  sourceFactory.toCMLString should be (
    "-Dinputs.anotherInputData.schema=s3 -Dinputs.anotherInputData.bucket=s3-bucket " +
      "-Dinputs.anotherInputData.pathPrefix=/another-source -Dinputs.anotherInputData.format=parquet " +
      "-Dinputs.anotherInputData.layout=hourly -Dinputs.anotherInputData.startDate=2018-09-01T00:00:00-0000 " +
      "-Dinputs.anotherInputData.endDate=2018-09-01T01:00:00-0000 " +
      "-Dinputs.textData.schema=cos -Dinputs.textData.bucket=test-bucket-span001 -Dinputs.textData.pathPrefix=/source " +
      "-Dinputs.textData.format=textFile -Dinputs.textData.layout=daily " +
      "-Dinputs.textData.startDate=2018-09-01T00:00:00-0000 -Dinputs.textData.endDate=2018-09-01T01:00:00-0000")

  val sinkFactory = new SinkFactory(defaultSettings)

  sinkFactory.toCMLString should be (
    "-Doutputs.wordCountData.schema=cos " +
      "-Doutputs.wordCountData.bucket=test-bucket-span001 " +
      "-Doutputs.wordCountData.pathPrefix=/word_count_sink " +
      "-Doutputs.wordCountData.format=parquet " +
      "-Doutputs.wordCountData.layout=customized " +
      "-Doutputs.wordCountData.startDate=2018-09-01T00:00:00-0000"
  )

  val appParams = new AppParams(defaultSettings)
  appParams.sparkConfigsToCMLString should be (
    "master=local[*] -conf spark.submit.deployMode=client -conf spark.sql.session.timeZone=UTC"
  )

  appParams.hadoopOptionsToCLMString should be (
    "-Dfs.s3a.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem " +
      "-Dfs.cos.ibmServiceName.access.key=changeme " +
      "-Dfs.cos.ibmServiceName.endpoint=changeme " +
      "-Dfs.stocator.scheme.list=cos " +
      "-Dfs.cos.ibmServiceName.iam.service.id=changeme " +
      "-Dfs.s3a.awsSecretAccessKey=changeme " +
      "-Dfs.cos.ibmServiceName.v2.signer.type=false " +
      "-Dfs.s3a.awsAccessKeyId=changeme " +
      "-Dfs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem " +
      "-Dfs.cos.ibmServiceName.secret.key=changeme " +
      "-Dfs.stocator.cos.scheme=cos " +
      "-Dfs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient"
  )

  appParams.toCMLString should be ("master=local[*] -conf spark.submit.deployMode=client " +
    "-conf spark.sql.session.timeZone=UTC --driver-java-options " +
    "'-Dfs.s3a.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem " +
    "-Dfs.cos.ibmServiceName.access.key=changeme " +
    "-Dfs.cos.ibmServiceName.endpoint=changeme " +
    "-Dfs.stocator.scheme.list=cos " +
    "-Dfs.cos.ibmServiceName.iam.service.id=changeme " +
    "-Dfs.s3a.awsSecretAccessKey=changeme " +
    "-Dfs.cos.ibmServiceName.v2.signer.type=false " +
    "-Dfs.s3a.awsAccessKeyId=changeme " +
    "-Dfs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem " +
    "-Dfs.cos.ibmServiceName.secret.key=changeme " +
    "-Dfs.stocator.cos.scheme=cos " +
    "-Dfs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient'")

  // Test for printing command line
  class TestAppClass extends BatchAppBase {
    run {
      print("Test")
    }
  }

  val testClass = new TestAppClass()
  testClass.getCommand should be (
    "spark-submit master=local[*] -conf spark.submit.deployMode=client -conf spark.sql.session.timeZone=UTC " +
      "--driver-java-options '-Denvironment=dev -DstartDate=2018-09-01T00:00:00-0000 -DendDate=2018-09-01T01:00:00-0000 " +
      "-Dfs.s3a.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem -Dfs.cos.ibmServiceName.access.key=changeme " +
      "-Dfs.cos.ibmServiceName.endpoint=changeme -Dfs.stocator.scheme.list=cos " +
      "-Dfs.cos.ibmServiceName.iam.service.id=changeme -Dfs.s3a.awsSecretAccessKey=changeme " +
      "-Dfs.cos.ibmServiceName.v2.signer.type=false -Dfs.s3a.awsAccessKeyId=changeme " +
      "-Dfs.cos.impl=com.ibm.stocator.fs.ObjectStoreFileSystem -Dfs.cos.ibmServiceName.secret.key=changeme " +
      "-Dfs.stocator.cos.scheme=cos -Dfs.stocator.cos.impl=com.ibm.stocator.fs.cos.COSAPIClient " +
      "-Dinputs.anotherInputData.schema=s3 -Dinputs.anotherInputData.bucket=s3-bucket " +
      "-Dinputs.anotherInputData.pathPrefix=/another-source -Dinputs.anotherInputData.format=parquet " +
      "-Dinputs.anotherInputData.layout=hourly -Dinputs.anotherInputData.startDate=2018-09-01T00:00:00-0000 " +
      "-Dinputs.anotherInputData.endDate=2018-09-01T01:00:00-0000 -Dinputs.textData.schema=cos " +
      "-Dinputs.textData.bucket=test-bucket-span001 -Dinputs.textData.pathPrefix=/source " +
      "-Dinputs.textData.format=textFile -Dinputs.textData.layout=daily -Dinputs.textData.startDate=2018-09-01T00:00:00-0000 " +
      "-Dinputs.textData.endDate=2018-09-01T01:00:00-0000 -Doutputs.wordCountData.schema=cos " +
      "-Doutputs.wordCountData.bucket=test-bucket-span001 -Doutputs.wordCountData.pathPrefix=/word_count_sink " +
      "-Doutputs.wordCountData.format=parquet -Doutputs.wordCountData.layout=customized " +
      "-Doutputs.wordCountData.startDate=2018-09-01T00:00:00-0000' --class [class name] [jar location]"
  )
}
