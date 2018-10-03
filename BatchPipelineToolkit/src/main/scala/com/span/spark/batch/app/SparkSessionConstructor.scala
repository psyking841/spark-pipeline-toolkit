package com.span.spark.batch.app

import org.apache.spark.sql.SparkSession

/**
  * A class for adding configurations to SparkSession Builder
  */
class SparkSessionConstructor(spark: SparkSession.Builder) {

  def configSparkWith(appParams: AppParams): SparkSession = {
    //Config SparkSession Builder (pre-sparksession construction)
    spark.master(appParams.sparkSessionOptionsMap.getOrElse("master", "local[*]"))
    appParams.sparkSessionOptionsMap.foreach { case (k, v) => spark.config(k, v) }

    //Config SparkSession (post-sparksession construction)
    lazy val postConstructor = new SparkSessionPostConstructor(spark.getOrCreate())

    appParams.hadoopOptionsMap.foreach{ case (k: String, v: String) => postConstructor.builder.withHadoopOption(k, v) }
    postConstructor.builder.getSparkSession()
  }

}

/**
  * A class for adding configurations to the active SparkSession or SparkContext
  */
class SparkSessionPostConstructor(spark: SparkSession) {

  val builder = new Builder()

  class Builder {
    def withHadoopOption(hadoopConfigKey: String, hadoopConfigValue: String): Builder = {
      spark.sparkContext.hadoopConfiguration.set(hadoopConfigKey, hadoopConfigValue)
      this
    }

    def getSparkSession(): SparkSession = {
      spark
    }
  }

}
