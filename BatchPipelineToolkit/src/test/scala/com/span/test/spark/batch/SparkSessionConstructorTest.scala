package com.span.test.spark.batch

import com.span.spark.batch.app.{AppParams, BatchAppSettings, SparkSessionConstructor}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class SparkSessionConstructorTest extends FlatSpec with Matchers {

  val settings = new BatchAppSettings //Will read the one inside library
  val constructor = new SparkSessionConstructor(SparkSession.builder())
  val appParams: AppParams = new AppParams(settings)
  val spark: SparkSession = constructor.configSparkWith(appParams)

  //Eval spark configurations
  spark.conf.get("master") should be ("local[*]")
  spark.conf.get("spark.submit.deployMode") should be ("client")
  spark.conf.get("spark.sql.session.timeZone") should be ("UTC")

  //Eval hadoop configurations
  spark.sparkContext.hadoopConfiguration.get("fs.cos.ibmServiceName.secret.key") should be ("changeme")
  spark.sparkContext.hadoopConfiguration.get("fs.cos.ibmServiceName.access.key") should be ("changeme")
  spark.sparkContext.hadoopConfiguration.get("fs.cos.ibmServiceName.endpoint") should be ("changeme")
  spark.sparkContext.hadoopConfiguration.get("fs.cos.ibmServiceName.iam.service.id") should be ("changeme")
  spark.sparkContext.hadoopConfiguration.get("fs.cos.ibmServiceName.v2.signer.type") should be ("false")
  spark.sparkContext.hadoopConfiguration.get("fs.stocator.scheme.list") should be ("cos")
  spark.sparkContext.hadoopConfiguration.get("fs.cos.impl") should be ("com.ibm.stocator.fs.ObjectStoreFileSystem")
  spark.sparkContext.hadoopConfiguration.get("fs.stocator.cos.impl") should be ("com.ibm.stocator.fs.cos.COSAPIClient")
  spark.sparkContext.hadoopConfiguration.get("fs.stocator.cos.scheme") should be ("cos")
  spark.sparkContext.hadoopConfiguration.get("fs.s3a.awsSecretAccessKey") should be ("changeme")
  spark.sparkContext.hadoopConfiguration.get("fs.s3a.awsAccessKeyId") should be ("changeme")
  spark.sparkContext.hadoopConfiguration.get("fs.s3a.impl") should be ("org.apache.hadoop.fs.s3native.NativeS3FileSystem")

}
