package com.pan.spark.batch.utils

import org.joda.time.DateTime

object TimeZoneUtils {

  /**
    * Return a query that filter the target table by startDate and endDate; these dates are converted to whatever timezone the table is using
    * @param dbTableName
    * @param selectColumns: if specific columns need to be selected
    * @param timeColumn: the column name on which we will filter the dataset, this is of timestamp type
    * @param startDate: the start date string passed from command line
    * @param endDate: the end date string passed from command line
    * @param tableTimeZone
    * @return
    */
  def getTimeFilteringQuery(dbTableName: String,
                            selectColumns: Seq[String],
                            timeColumn: String,
                            startDate: DateTime,
                            endDate: DateTime,
                            tableTimeZone: String): String = {
    //We convert the start date and end date to the same timezone as the table resides.
    val startDateStr = Utils.displayDateTimeInTimeZone(startDate, "yyyy-MM-dd-HH-mm-ss", tableTimeZone)

    val endDateStr = Utils.displayDateTimeInTimeZone(endDate, "yyyy-MM-dd-HH-mm-ss", tableTimeZone)

    val selectedCols: String =
                    if(selectColumns.isEmpty)
                      "*"
                    else if(selectColumns.head == "DISTINCT")
                      "DISTINCT " + selectColumns.slice(1, selectColumns.size).reduce((s1, s2) => s1 + ", " + s2)
                    else
                      selectColumns.reduce((s1, s2) => s1 + ", " + s2)

    s"""
      | SELECT $selectedCols FROM $dbTableName
      | WHERE $timeColumn >= to_timestamp('$startDateStr', 'YYYY-MM-DD-HH24-MI-SS')
      |   AND $timeColumn < to_timestamp('$endDateStr', 'YYYY-MM-DD-HH24-MI-SS')
    """.stripMargin
  }

  /**
    * This method replaces the character 'T', ' ', and ':' to '-'; for example, 2018-12-02T00:00:00 will be 2018-12-02-00-00-00.
    * This is to avoid the syntax error when passing in String with T or space to sql query.
    * @param dateStr
    * @return
    */
  def convertDateFormat(dateStr: String): String = {
    dateStr.trim.replaceAll("T|:| ", "-")
  }

  /**
    * Covert timestamp string in the table from its timezone to another
    * @param timeStr
    * @param pattern
    * @param oldTimeZone: the old timeZone
    * @param newTimeZone: the new timeZone
    * @return
    */
  def convertTimeZone(timeStr: String, pattern: String, oldTimeZone: String, newTimeZone: String, newPattern: String = null): String = {
    val date: DateTime = Utils.getDateTimeInTimeZone(timeStr, pattern, oldTimeZone)
    if(newPattern == null) Utils.displayDateTimeInTimeZone(date, pattern, newTimeZone)
    else Utils.displayDateTimeInTimeZone(date, newPattern, newTimeZone)
  }
}
