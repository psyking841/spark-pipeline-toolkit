package com.pan.spark.batch.datasinks

import org.apache.spark.sql.DataFrame

/**
  * Root trait for entities which persist instance of the speicified type.
  */
trait Sink {

  /**
    * Emit the given value.
    *
    * @param value  Object to emit
    */
  def emit: DataFrame => Unit
}
