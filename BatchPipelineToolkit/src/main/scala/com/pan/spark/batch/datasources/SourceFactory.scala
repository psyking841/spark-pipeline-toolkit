package com.pan.spark.batch.datasources

import com.pan.spark.batch.Params
import com.pan.spark.batch.app.BatchAppSettings
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

class SourceFactory(settings: BatchAppSettings) {

  val allDatasets: Seq[String] = {
    settings.defaultConfigs.getConfig("inputs").root().entrySet().map(_.getKey).toList
  }

  val paramsMap: Map[String, Params] = (allDatasets zip allDatasets.map(buildParam(_: String))).toMap

  def buildParam(dataset: String): Params = {
    val datasetConf = settings.inputsConfigs(dataset)
    val schema = datasetConf.getString("schema")

    schema match {
      case "s3" | "cos" => {
        new CloudStorageSourceParams(datasetConf, dataset)
      }
    }
  }

  def toCMLString: String = {
    paramsMap.values.map(_.toCMLString).mkString(" ")
  }

  /**
    * A method that returns different Source objects based on the setting in the config
    * @param dataset
    * @return
    */
  def getDataSource(dataset: String)(spark: SparkSession): Source = {
    val datasetConf = settings.inputsConfigs(dataset)
    val schema = datasetConf.getString("schema")

    schema match {
      case "s3" | "cos" => {
        new CloudStorageSource(paramsMap(dataset).asInstanceOf[CloudStorageSourceParams])(spark)
      }
    }

  }
}
