package com.pan.spark.batch.datasinks

import com.pan.spark.batch.Params
import com.pan.spark.batch.app.BatchAppSettings
import scala.collection.JavaConversions._

class SinkFactory(settings: BatchAppSettings) {

  val allDatasets: Seq[String] = {
    settings.defaultConfigs.getConfig("outputs").root().entrySet().map(_.getKey).toList
  }

  val paramsMap: Map[String, Params] = (allDatasets zip allDatasets.map(buildParam(_: String))).toMap

  def buildParam(dataset: String): Params = {
    val datasetConf = settings.outputsConfigs(dataset)
    val schema = datasetConf.getString("schema")

    schema match {
      case "s3" | "cos" => {
        new CloudStorageSinkParams(datasetConf, dataset)
      }
    }
  }

  def toCMLString: String = {
    paramsMap.values.map(_.toCMLString).mkString(" ")
  }

  /**
    * A method that returns Source object
    * @param dataset
    * @return
    */
  def getDataSink(dataset: String): Sink = {

    val datasetConf = settings.outputsConfigs(dataset)
    val schema = datasetConf.getString("schema")

    schema match {
      case "s3" | "cos" => {
        new CloudStorageSink(paramsMap(dataset).asInstanceOf[CloudStorageSinkParams])
      }
    }

//    val datasetConf = settings.getOutputsConfigs(dataset)
//    //take default saving mode if no saving mode is specified for this output data
//    val saveMode = datasetConf.getString("saveMode")
//    val startDate: DateTime = Utils.getDateTime(datasetConf.getString("startDate"))
//
//    //if (datasetConf.getString("format") == "ibmcos") {
//    val sinkUriBuilder = SinkUriBuilder.builder()
//    //Building input paths or directories
//    sinkUriBuilder.withSchema(datasetConf.getString("schema"))
//      .withBucket(datasetConf.getString("bucket"))
//      .withPathPrefix(datasetConf.getString("pathPrefix"))
//      .withStartDate(startDate)
//      .withLayout(datasetConf.getString("layout"))
//
//    val outputPath = sinkUriBuilder.build().getPathUri()
//
//    val sinkBuilder = CloudStorageSink.builder()
//    // If customized partitionColumns is provided, output director will not using the default data paritioning which uses the startDate and endDate
//    sinkBuilder.withOutputDirectory(outputPath.toString)
//      .withFormat(datasetConf.getString("format")) //default to parquet output format which is set in conf file depending on different environments,
//      .withSaveMode(saveMode) //default to append as defined in conf file; this can be changed from command line
//      .build()
  }

}
