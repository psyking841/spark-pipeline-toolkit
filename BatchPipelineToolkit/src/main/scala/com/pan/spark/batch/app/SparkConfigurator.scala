package com.pan.spark.batch.app

import org.apache.spark.sql.SparkSession

/**
  * A class for adding configurations to SparkSession or SparkContext
  */
class SparkConfigurator(spark: SparkSession) {

  var builder: SparkSessionBuilder = new SparkSessionBuilder(spark)

  def getConfigedSparkWith(appParams: AppParams): SparkSession = {
    val configMap = Map(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
      "fs.stocator.scheme.list" -> "cos",
      "fs.cos.impl" -> "com.ibm.stocator.fs.ObjectStoreFileSystem",
      "fs.stocator.cos.impl" -> "com.ibm.stocator.fs.cos.COSAPIClient",
      "fs.stocator.cos.scheme" -> "cos",
      "fs.cos.myCos.v2.signer.type" -> "false"
    )

    //Config SparkSession
    (configMap ++ appParams.sparkOptionsMap)
      .foreach{ case (k: String, v: String) => builder = builder.withHadoopOption(k, v) }
    builder.build()
  }

}

object SparkConfigurator {
  def apply(spark: SparkSession): SparkConfigurator ={
    new SparkConfigurator(spark)
  }
}


class SparkSessionBuilder(spark: SparkSession) {

  def withHadoopOption(hadoopConfigKey: String, hadoopConfigValue: String): SparkSessionBuilder = {
    spark.sparkContext.hadoopConfiguration.set(hadoopConfigKey, hadoopConfigValue)
    this
  }

  def build(): SparkSession = {
    spark
  }
}
