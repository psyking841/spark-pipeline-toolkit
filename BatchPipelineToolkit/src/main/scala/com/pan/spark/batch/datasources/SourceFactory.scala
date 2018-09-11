package com.pan.spark.batch.datasources

import com.pan.spark.batch.app.BatchAppSettings
import com.pan.spark.batch.utils.cloudstorage.SourceUriBuilder
import com.pan.spark.batch.utils. Utils
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

class SourceFactory(spark: SparkSession, settings: BatchAppSettings) {

  /**
    * A method that returns different Source objects based on the setting in the config
    * @param dataset
    * @return
    */
  def getDataSource(dataset: String): Source = {
    val datasetConf = settings.getInputsConfigs(dataset)
    var startDate: DateTime = Utils.getDateTimeInTimeZone(datasetConf.getString("startDate"))
    var endDate: DateTime = Utils.getDateTimeInTimeZone(datasetConf.getString("endDate"))

    //If the user specify delta on start and end date, take that!
    if(datasetConf.hasPath("startDateDelta"))
      startDate = Utils.getDateWithDelta(startDate, datasetConf.getString("startDateDelta"))

    if(datasetConf.hasPath("endDateDelta"))
      endDate = Utils.getDateWithDelta(startDate, datasetConf.getString("startDateDelta"))

    //return CosSource instance
    val sourceUriBuilder = SourceUriBuilder.builder()
    //Building input paths or directories
    sourceUriBuilder.withSchema(datasetConf.getString("schema"))
      .withBucket(datasetConf.getString("bucket"))
      .withPathPrefix(datasetConf.getString("pathPrefix"))
      .withStartDate(startDate)
      .withEndDate(endDate)
      .withLayout(datasetConf.getString("layout"))

    val inputPaths: Seq[String] = sourceUriBuilder.build().getPathUriSet().map(_.toString)

    //Construct Spark Data Frame from input paths or directories
    CloudStorageSource.builder()
      .withSparkSession(spark)
      .withFormat(datasetConf.getString("format"))
      .withDirectory(inputPaths)
      .build()
  }
}

object SourceFactory {
  def apply(spark: SparkSession, settings: BatchAppSettings): SourceFactory ={
    new SourceFactory(spark, settings)
  }
}
