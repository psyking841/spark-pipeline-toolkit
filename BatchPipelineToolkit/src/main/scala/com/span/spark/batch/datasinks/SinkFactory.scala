package com.span.spark.batch.datasinks

import com.span.spark.batch.Params
import com.span.spark.batch.app.BatchAppSettings

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
  }

}
