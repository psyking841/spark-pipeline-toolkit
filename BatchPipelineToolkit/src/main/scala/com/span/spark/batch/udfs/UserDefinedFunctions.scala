package com.span.spark.batch.udfs

import com.span.spark.batch.utils.TimeZoneUtils

import org.apache.spark.sql.functions.udf

object UserDefinedFunctions {
  def convertTimeZoneUDF = udf(TimeZoneUtils.convertTimeZone(_: String, _: String, _: String, _: String))
  def convertTimeZoneWithNewPatternUDF = udf(TimeZoneUtils.convertTimeZone(_: String, _: String, _: String, _: String, _: String))
}
