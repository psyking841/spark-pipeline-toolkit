package com.pan.spark.batch.datasinks

import org.apache.spark.sql.DataFrame

class CloudStorageSink extends Sink {
  private var directory: String = _
  private var saveMode: String = _
  private var format: String = _

  /**
    * This method gives the user the flexibility to override the partition columns in the code
    * @param partition a string of column names separated by comma, such as "year, month, day"
    * @param value the data frame that is going to be published
    */
  def emit(partition: Seq[String])(value: DataFrame): Unit = {
    if(partition.nonEmpty){
      value.write.mode(saveMode).format(format).partitionBy(partition: _*).save(directory)
    } else {
      value.write.mode(saveMode).format(format).save(directory)
    }
  }

  def emit: DataFrame => Unit = emit(Seq())
}

object CloudStorageSink {

  def builder(): Builder = new Builder

  class Builder {
    private val sink: CloudStorageSink = new CloudStorageSink()

    def withSaveMode(saveMode: String): Builder = {
      sink.saveMode = saveMode
      this
    }

    def withFormat(format: String): Builder = {
      sink.format = format
      this
    }

    def withOutputDirectory(directory: String): Builder = {
      sink.directory = directory
      this
    }

    def build(): CloudStorageSink = {
      return sink
    }
  }
}
