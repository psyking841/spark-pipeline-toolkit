package com.pan.spark.batch.datasinks

import com.pan.spark.batch.Params
import com.typesafe.config.Config
import com.pan.spark.batch.utils.Utils
import com.pan.spark.batch.utils.cloudstorage.SinkUriBuilder
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

class CloudStorageSinkParams(datasetConf: Config, datasetName: String) extends Params {
  /**
    * Schema of the source: s3 or cos
    */
  var schema: String = datasetConf.getString("schema")

  /**
    * Bucket of the source data
    */
  var bucket: String = datasetConf.getString("bucket")

  /**
    * ABSOLUTE path of the source data (after the bucket name)
    */
  var pathPrefix: String = datasetConf.getString("pathPrefix")

  /**
    * The format of source data: json, parquet etc.
    */
  var format: String = datasetConf.getString("format")

  /**
    * The layout of source data: daily, hourly etc.
    */
  var layout: String = datasetConf.getString("layout")

  /**
    * Optional; if provided, the schema will be applied to the sourced dataframe
    */
  var datasetSchema: Option[StructType] = None

  /**
    * Upper bound of the source data; this will be used to name the path if layout is
    */
  var startDate: DateTime = {
    val sd = Utils.getDateTimeInTimeZone(datasetConf.getString("startDate"))
    if (datasetConf.hasPath("startDateDelta")) {
      Utils.getDateWithDelta(sd, datasetConf.getString("startDateDelta"))
    } else {
      sd
    }
  }

  /**
    * Derive output path for source data; this is different from input paths in that it only outputs to one directory
    */
  lazy val outputPath: String = {
    val sinkUriBuilder = SinkUriBuilder.builder()
    //Building output paths or directories
    sinkUriBuilder.withSchema(schema)
      .withBucket(bucket)
      .withPathPrefix(pathPrefix)
      .withStartDate(startDate)
      .withLayout(layout)
    sinkUriBuilder.build().getPathUri().toString
  }

  override def toCMLString: String = {
    "-Doutputs." + datasetName + ".schema=" + schema + " " +
    "-Doutputs." + datasetName + ".bucket=" + bucket + " " +
    "-Doutputs." + datasetName + ".pathPrefix=" + pathPrefix + " " +
    "-Doutputs." + datasetName + ".format=" + format + " " +
    "-Doutputs." + datasetName + ".layout=" + layout + " " +
    "-Doutputs." + datasetName + ".startDate=" + datasetConf.getString("startDate")
  }
}
