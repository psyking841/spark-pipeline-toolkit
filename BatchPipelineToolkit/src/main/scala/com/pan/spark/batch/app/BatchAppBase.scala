package com.pan.spark.batch.app

import com.pan.spark.batch.datasinks.{Sink, SinkFactory}
import com.pan.spark.batch.datasources.{Source, SourceFactory}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Base class for all Spark batch applications to extend.
  * It manages the configurations (default ones and inputs/outputs specific ones)
  * and provides the Spark application developer syntax sugar for sourcing and sinking datasets
  */
abstract class BatchAppBase extends App {

  /**
    * A container for all configurations for input datasets, output datasets and spark
    */
  protected val defaultSettings = new BatchAppSettings()

  val appParams: AppParams = new AppParams(defaultSettings)

  /**
    * Store default configurations for configuring SparkSession
    */
  final val logger = LogManager.getLogger(getClass.getName)

  /**
    * SparkSession variable with configurations from config file
    */
  lazy val spark: SparkSession = {
    val sessionBuilder = SparkSession.builder().config("spark.sql.session.timeZone", "UTC")

    //In case running in local
    if (defaultSettings.defaultConfigs.getString("environment") == "local") {
      sessionBuilder.master("local[*]")
    }

    //Adding customized configurations to SparkSession
    SparkConfigurator(sessionBuilder.getOrCreate()).getConfigedSparkWith(appParams)
  }

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

  def getCommand(): String = {
    "spark-submit --driver-java-options '" + appParams.asJavaOptions() + " " +
      sourceFactory.paramsAsJavaOptions() + " " + sinkFactory.paramsAsJavaOptions() +
      "' --class [class name] [jar location]"
  }

  /**
    * Execute the batch transformation
    * @param batchprocessorFunction
    */
  def run(batchprocessorFunction: => Unit): Unit = {
    //Writes
    batchprocessorFunction
  }

}
