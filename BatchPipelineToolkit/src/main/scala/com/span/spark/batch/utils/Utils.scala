package com.span.spark.batch.utils

import java.nio.file.{Path, Paths}

import org.joda.time.{DateTime, DateTimeZone, Period}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Utils {

  /**
    * Get now in given format
    * @return
    */
  def now: String = displayDateTimeInTimeZone(DateTime.now(DateTimeZone.UTC), "yyyy-MM-dd'T'HH:mm:ssZ")

  /**
    * Convert a date string in a specific pattern to DateTime object
    * @param strDateTime
    * @param inPattern; by default it is yyyy-MM-dd'T'HH:mm:ssZ, which is used by command line start/end dates
    * @param timeZone; by default the resulting datetime object is in UTC, which is used by command line start/end dates
    * @return
    */
  def getDateTimeInTimeZone(strDateTime: String,
                            inPattern: String = "yyyy-MM-dd'T'HH:mm:ssZ",
                            timeZone: String = "UTC"): DateTime = {
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(inPattern)
    val newFormatter = formatter.withZone(DateTimeZone.forID(timeZone))
    newFormatter.parseDateTime(strDateTime)
  }

  /**
    * Display a given datetime object in string in given timezone with given pattern
    * @param dateTime
    * @param displayInPattern
    * @param timeZone
    * @return
    */
  def displayDateTimeInTimeZone(dateTime: DateTime, displayInPattern: String, timeZone: String = "UTC"): String = {
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(displayInPattern)
    val newFormatter = formatter.withZone(DateTimeZone.forID(timeZone))
    newFormatter.print(dateTime)
  }

  /**
    * Parse date in String type to object of DateTime
    * @param strDateTime
    * @return
    */
  def getDateTime(strDateTime: String, pattern: String = "yyyy-MM-dd'T'HH:mm:ssZ"): DateTime = {
    val formatter = DateTimeFormat.forPattern(pattern)
    formatter.parseDateTime(strDateTime)
  }

  /**
    * Get the full path (without scheme, i.e. cos:// or s3a://) to the dataset; note that the output path is timezone awared.
    * The full path is formated in such a way path/to/dataset/y=2017/m=12/d=03/h=09
    * @param prefix
    * @param dateTime
    * @param timeZoneId
    * @param layout
    * @return
    */
  def formatFullPath(prefix: Path, dateTime: DateTime, layout: String, timeZoneId: String = "UTC"): Path = {
    val dateInTZ: DateTime = dateTime.withZone(DateTimeZone.forID(timeZoneId))
    val y = dateInTZ.getYear
    val m = dateInTZ.getMonthOfYear
    val d = dateInTZ.getDayOfMonth
    val h = dateInTZ.getHourOfDay
    if(layout.equals("hourly")){
      Paths.get(prefix.toString,
        "/year=" + f"$y%4d",
        "/month=" + f"$m%02d",
        "/day=" + f"$d%02d",
        "/hour=" + f"$h%02d")
    }else if(layout.equals("daily")){
      Paths.get(prefix.toString,
        "/year=" + f"$y%4d",
        "/month=" + f"$m%02d",
        "/day=" + f"$d%02d")
    }else{
      Paths.get(prefix.toString)
    }
  }

  /**
    * Convert path prefix to full path. This is typically used in the case where custom partition is uesed
    * @param prefix
    * @return
    */
  def formatFullPath(prefix: Path): Path = {
    Paths.get(prefix.toString)
  }

  /**
    * Get a sequence of input paths between startDate and endDate and are formatted using formatFullPath function
    * @param inputPathPrefix
    * @param startDate
    * @param endDate
    * @param timeZoneId
    * @param layout: daily or hourly or none; if daily, only y=1111/m=2/d=3 will be in the path; if hourly, y=1111/m=2/d=3/h=4 will be in the path
    * @return
    */
  def getFullPaths(pathPrefix: Path, startDate: DateTime, endDate: DateTime, layout: String, timeZoneId: String="UTC"): Seq[Path] = {
    var results: Seq[Path] = Seq()
    var start = startDate
    while (start.isBefore(endDate)) {
      results = results :+ formatFullPath(pathPrefix, start, layout, timeZoneId)
      start = start.plus(Period.hours(1))
    }
    results
  }

  /**
    * Modify the given date based on delta
    * @param date
    * @param deltaStr
    * @return
    */
  def getDateWithDelta(date: DateTime, deltaStr: String): DateTime = {
    val deltaPattern =  "(-?[0-9]+)\\s*([A-Za-z]+)".r
    val deltaPattern(delta, unit) = deltaStr
    val deltaInt = Integer.parseInt(delta)
    unit match {
      case "day" | "days" => date.plusDays(deltaInt)
      case "hour" | "hours" => date.plusHours(deltaInt)
    }

  }
}
