package com.pan.spark.batch.datasources

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class CloudStorageSource extends Source {
  private var ss: SparkSession = _
  private var format: String = _
  private var datasetSchema: Option[StructType] = None
  private var datasetDirectories: Seq[String] = Seq()

  /**
    * Source all files in parquet format under the folder; the schema of the input data will be inferred automatically by Spark
    * @return
    */
  def source: DataFrame = {
    datasetSchema match {
      case Some(s) => {
        format match {
          case "json" => ss.read.schema(s).json(datasetDirectories: _*)
          case _ => ss.read.schema(s).parquet(datasetDirectories: _*)
        }
      }
      case None => format match {
        case "json" => ss.read.json(datasetDirectories: _*)
        case "text" => ss.read.textFile(datasetDirectories: _*).toDF()
        case _ => ss.read.parquet(datasetDirectories: _*)
      }
    }
  }
}

object CloudStorageSource {

  def builder(): Builder = new Builder

  class Builder {
    private val source: CloudStorageSource = new CloudStorageSource()

    def withSparkSession(ss: SparkSession): Builder = {
      source.ss = ss
      this
    }

    def withDirectory(directories: Seq[String]): Builder = {
      source.datasetDirectories = directories
      this
    }

    def withDatasetSchema(schema: StructType): Builder = {
      source.datasetSchema = Option(schema)
      this
    }

    def withFormat(format: String): Builder = {
      source.format = format
      this
    }

    def build(): CloudStorageSource = {
      return source
    }
  }
}
