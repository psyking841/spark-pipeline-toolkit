package com.span.spark.batch.datasources

import org.apache.spark.sql.{DataFrame, SparkSession}

class CloudStorageSource(params: CloudStorageSourceParams)(ss: SparkSession) extends Source {

  /**
    * Source all files in parquet format under the folder;
    * the schema of the input data will be inferred automatically by Spark
    * @return DataFrame
    */
  def source: DataFrame = {
    params.datasetSchema match {
      case Some(s) => {
        params.format match {
          case "json" => ss.read.schema(s).json(params.inputPaths: _*)
          case "text" => ss.read.textFile(params.inputPaths: _*).toDF()
          case _ => ss.read.schema(s).parquet(params.inputPaths: _*)
        }
      }
      case None => ss.read.parquet(params.inputPaths: _*)
    }
  }

}
