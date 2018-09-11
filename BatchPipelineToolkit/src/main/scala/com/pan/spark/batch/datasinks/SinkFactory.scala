package com.pan.spark.batch.datasinks

import com.pan.spark.batch.app.BatchAppSettings
import com.pan.spark.batch.utils.cloudstorage.SinkUriBuilder
import com.pan.spark.batch.utils.Utils
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

class SinkFactory(spark: SparkSession, settings: BatchAppSettings) {

  /**
    * A method that returns Source object
    * @param dataset
    * @return
    */
  def getDataSink(dataset: String): Sink = {

    val datasetConf = settings.getOutputsConfigs(dataset)
    //take default saving mode if no saving mode is specified for this output data
    val saveMode = datasetConf.getString("saveMode")
    val startDate: DateTime = Utils.getDateTime(datasetConf.getString("startDate"))

    //if (datasetConf.getString("format") == "ibmcos") {
    val sinkUriBuilder = SinkUriBuilder.builder()
    //Building input paths or directories
    sinkUriBuilder.withSchema(datasetConf.getString("schema"))
      .withBucket(datasetConf.getString("bucket"))
      .withPathPrefix(datasetConf.getString("pathPrefix"))
      .withStartDate(startDate)
      .withLayout(datasetConf.getString("layout"))

    val outputPath = sinkUriBuilder.build().getPathUri()

    val sinkBuilder = CloudStorageSink.builder()
    // If customized partitionColumns is provided, output director will not using the default data paritioning which uses the startDate and endDate
    sinkBuilder.withOutputDirectory(outputPath.toString)
      .withFormat(datasetConf.getString("format")) //default to parquet output format which is set in conf file depending on different environments,
      .withSaveMode(saveMode) //default to append as defined in conf file; this can be changed from command line
      .build()
  }

}

object SinkFactory {
  def apply(spark: SparkSession, settings: BatchAppSettings): SinkFactory = {
    new SinkFactory(spark, settings)
  }
}
