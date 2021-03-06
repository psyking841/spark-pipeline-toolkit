package com.span.spark.batch.datasources

import com.span.spark.batch.Params
import com.span.spark.batch.utils.Utils
import com.span.spark.batch.utils.cloudstorage.SourceUriBuilder

import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

class CloudStorageSourceParams(datasetConf: Config, datasetName: String) extends Params {
  /**
    * Schema of the source: s3 or cos
    */
  lazy val schema: String = datasetConf.getString("schema")

  /**
    * Bucket of the source data
    */
  lazy val bucket: String = datasetConf.getString("bucket")

  /**
    * ABSOLUTE path of the source data (after the bucket name)
    */
  lazy val pathPrefix: String = datasetConf.getString("pathPrefix")

  /**
    * The format of source data: json, parquet etc.
    */
  lazy val format: String = datasetConf.getString("format")

  /**
    * The layout of source data: daily, hourly etc.
    */
  lazy val layout: String = datasetConf.getString("layout")

  /**
    * Optional; if provided, the schema will be applied to the sourced dataframe
    */
  lazy val datasetSchema: Option[StructType] = None

  /**
    * Upper bound of the source data
    */
  lazy val startDate: DateTime = {
    val sd = Utils.getDateTimeInTimeZone(datasetConf.getString("startDate"))
    if (datasetConf.hasPath("startDateDelta")) {
      Utils.getDateWithDelta(sd, datasetConf.getString("startDateDelta"))
    } else {
      sd
    }
  }

  /**
    * Lower bound of the source data
    */
  lazy val endDate: DateTime = {
    val ed = Utils.getDateTimeInTimeZone(datasetConf.getString("endDate"))
    if (datasetConf.hasPath("endDateDelta")) {
      Utils.getDateWithDelta(ed, datasetConf.getString("endDateDelta"))
    } else {
      ed
    }
  }

  /**
    * Derive input paths for source data
    */
  lazy val inputPaths: Seq[String] = {
    val sourceUriBuilder = SourceUriBuilder.builder()
    //Building input paths or directories
    sourceUriBuilder.withSchema(schema)
      .withBucket(bucket)
      .withPathPrefix(pathPrefix)
      .withStartDate(startDate)
      .withEndDate(endDate)
      .withLayout(layout)
    sourceUriBuilder.build().getPathUriSet().map(_.toString)
  }

  override def toCMLString: String = {
    "-Dinputs." + datasetName + ".schema=" + schema + " " +
    "-Dinputs." + datasetName + ".bucket=" + bucket + " " +
    "-Dinputs." + datasetName + ".pathPrefix=" + pathPrefix + " " +
    "-Dinputs." + datasetName + ".format=" + format + " " +
    "-Dinputs." + datasetName + ".layout=" + layout + " " +
    "-Dinputs." + datasetName + ".startDate=" + datasetConf.getString("startDate") + " " +
    "-Dinputs." + datasetName + ".endDate=" + datasetConf.getString("endDate")
  }
}
