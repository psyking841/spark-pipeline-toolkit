package com.pan.spark.batch.app

import org.apache.spark.sql.SparkSession

/**
  * A class for adding configurations to SparkSession Builder
  */
class SparkSessionConstructor(spark: SparkSession.Builder) {

  def getConfigedSparkWith(appParams: AppParams): SparkSession = {
    //Config SparkSession Builder (pre-construction)
    spark.master(appParams.sparkSessionOptionsMap.getOrElse("master", "local[*]"))
    appParams.sparkSessionOptionsMap.foreach { case (k, v) => spark.config(k, v) }

    //Config SparkSession (post-construction)
    lazy val postConstructor: SparkSessionPostConstructor = new SparkSessionPostConstructor(spark.getOrCreate())

//    val configMap = Map(
//      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
//      "fs.stocator.scheme.list" -> "cos",
//      "fs.cos.impl" -> "com.ibm.stocator.fs.ObjectStoreFileSystem",
//      "fs.stocator.cos.impl" -> "com.ibm.stocator.fs.cos.COSAPIClient",
//      "fs.stocator.cos.scheme" -> "cos",
//      "fs.cos.myCos.v2.signer.type" -> "false"
//    )

    appParams.fsOptionsMap.foreach{ case (k: String, v: String) => postConstructor.withHadoopOption(k, v) }
    postConstructor.build()
  }

}

object SparkSessionConstructor {
  def apply(spark: SparkSession.Builder): SparkSessionConstructor ={
    new SparkSessionConstructor(spark)
  }
}

/**
  * A class for adding configurations to the active SparkSession or SparkContext
  */
class SparkSessionPostConstructor(var spark: SparkSession) {
  def withHadoopOption(hadoopConfigKey: String, hadoopConfigValue: String): SparkSessionPostConstructor = {
    spark.sparkContext.hadoopConfiguration.set(hadoopConfigKey, hadoopConfigValue)
    this
  }

  def build(): SparkSession = {
    spark
  }
}
