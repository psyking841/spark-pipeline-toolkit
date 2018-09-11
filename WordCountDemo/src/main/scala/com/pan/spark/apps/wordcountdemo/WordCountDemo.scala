package com.pan.spark.apps.wordcountdemo

import java.util.Calendar

import com.pan.spark.batch.app.BatchAppBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WordCountDemo extends BatchAppBase {

  import spark.implicits._

  run {
    val cosTextFile: DataFrame = getDataSourceFor("textData").source
    val wordCounts = cosTextFile.flatMap(line => line.toString.split(" ")).groupByKey(identity).count
    //Get current date
    val now = Calendar.getInstance()
    val y = now.get(Calendar.YEAR)
    val m = now.get(Calendar.MONTH)
    val d = now.get(Calendar.DAY_OF_MONTH)

    val wordCountsDf = wordCounts.toDF("word", "count")
      .withColumn("year", lit(y))
      .withColumn("month", lit(m + 1))
      .withColumn("day", lit(d))
      .coalesce(1)

    getDataSinkFor("wordCountData").emit(wordCountsDf)
  }

}
