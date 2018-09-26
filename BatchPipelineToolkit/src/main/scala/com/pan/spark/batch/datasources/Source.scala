package com.pan.spark.batch.datasources

import org.apache.spark.sql.DataFrame

trait Source {

  def source: DataFrame

}
