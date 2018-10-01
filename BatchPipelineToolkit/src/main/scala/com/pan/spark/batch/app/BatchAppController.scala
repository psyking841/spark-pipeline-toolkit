package com.pan.spark.batch.app

import com.pan.spark.batch.datasinks.{Sink, SinkFactory}
import com.pan.spark.batch.datasources.{Source, SourceFactory}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * This class manages the configurations (default ones and inputs/outputs specific ones)
  * and provides the Spark application developer syntax sugar for sourcing and sinking datasets
  */
class BatchAppController {

  /**
    * A container for all configurations for input datasets, output datasets and spark
    */
  protected val defaultSettings = new BatchAppSettings()

  protected val appParams: AppParams = new AppParams(defaultSettings)

  /**
    * Store default configurations for configuring SparkSession
    */
  final val logger = LogManager.getLogger(getClass.getName)

  /**
    * Configure SparkSession with configurations from config file
    */
  lazy val spark: SparkSession = SparkSessionConstructor(SparkSession.builder).getConfigedSparkWith(appParams)

  /**
    * Make SparkContext available to children objects
    */
  lazy val sc: SparkContext = spark.sparkContext

  /**
    * A factory object for getting different types of sources
    */
  private val sourceFactory = new SourceFactory(defaultSettings)

  /**
    * A factory object for getting different types of sinks
    */
  private val sinkFactory = new SinkFactory(defaultSettings)

  /**
    * Sourcing data based on the inputFormat
    */
  def getDataSourceFor(dataset: String): Source = {
    sourceFactory.getDataSource(dataset)(spark)
  }

  /**
    * Sinking data based on outputFormat
    */
  def getDataSinkFor(dataset: String): Sink = {
    sinkFactory.getDataSink(dataset)
  }

  def getCommand: String = {
    val envParam: String =
      if(defaultSettings.defaultConfigs.hasPath("environment"))
        defaultSettings.defaultConfigs.getString("environment")
      else "[environment]"

    val startDate: String = if(defaultSettings.defaultConfigs.hasPath("startDate"))
      defaultSettings.defaultConfigs.getString("startDate")
    else "[startDate]"

    val endDate: String = if(defaultSettings.defaultConfigs.hasPath("endDate"))
      defaultSettings.defaultConfigs.getString("endDate")
    else "[endDate]"

    "spark-submit " + appParams.sparkConfigsToCMLString + " --driver-java-options '" + "-Denvironment=" + envParam +
      " -DstartDate=" + startDate + " -DendDate=" + endDate + " " + appParams.hadoopOptionsToCLMString +
      " " + sourceFactory.toCMLString + " " + sinkFactory.toCMLString +
      "' --class [class name] [jar location]"
  }

}
