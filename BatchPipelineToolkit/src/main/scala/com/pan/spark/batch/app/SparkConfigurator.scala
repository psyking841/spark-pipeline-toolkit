package com.pan.spark.batch.app

import org.apache.spark.sql.SparkSession

/**
  * A class for adding configurations to SparkSession or SparkContext
  */
class SparkConfigurator(spark: SparkSession) {

  var builder: SparkSessionBuilder = new SparkSessionBuilder(spark)

  def getConfigedSparkWith(settings: BatchAppSettings): SparkSession = {
    val configMap = scala.collection.mutable.Map(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
      "fs.stocator.scheme.list" -> "cos",
      "fs.cos.impl" -> "com.ibm.stocator.fs.ObjectStoreFileSystem",
      "fs.stocator.cos.impl" -> "com.ibm.stocator.fs.cos.COSAPIClient",
      "fs.stocator.cos.scheme" -> "cos",
      "fs.cos.myCos.v2.signer.type" -> "false"
    )

    var ibmServiceName: String = "defaultServiceName"
    if(settings.getDefaultConfigs.hasPath("ibmServiceName")) {
       ibmServiceName = settings.getDefaultConfigs.getString("ibmServiceName")
    }

    if(settings.getDefaultConfigs.hasPath("endpoint")){
      configMap += ("fs.cos." + ibmServiceName + ".endpoint" -> settings.getDefaultConfigs.getString("endpoint"))
    }

    if(settings.getDefaultConfigs.hasPath("serviceId")){
      configMap += ("fs.cos." + ibmServiceName + ".iam.service.id" -> settings.getDefaultConfigs.getString("serviceId"))
    }

    if(settings.getDefaultConfigs.hasPath("ibmCosAccessKey")){
      configMap += ("fs.cos." + ibmServiceName + ".access.key" -> settings.getDefaultConfigs.getString("ibmCosAccessKey"))
    }

    if(settings.getDefaultConfigs.hasPath("ibmCosSecretKey")){
      configMap += ("fs.cos" + ibmServiceName + "secret.key" -> settings.getDefaultConfigs.getString("ibmCosSecretKey"))
    }

    if(settings.getDefaultConfigs.hasPath("awsKeyId")){
      configMap += ("fs.s3a.awsAccessKeyId" -> settings.getDefaultConfigs.getString("awsKeyId"))
    }

    if(settings.getDefaultConfigs.hasPath("awsSecretKey")){
      configMap += ("fs.s3a.awsSecretAccessKey" -> settings.getDefaultConfigs.getString("awsSecretKey"))
    }

    //Config SparkSession
    configMap.foreach{ case (k: String, v: String) => builder = builder.withHadoopOption(k, v) }
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
