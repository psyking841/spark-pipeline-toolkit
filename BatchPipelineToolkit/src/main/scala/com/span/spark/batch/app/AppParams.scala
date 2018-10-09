package com.span.spark.batch.app

import com.span.spark.batch.Params

import scala.collection.JavaConversions._

/**
  * This class includes methods that convert settings into internal maps for configuring Spark and Hadoop
  * @param settings
  */
class AppParams(settings: BatchAppSettings) extends Params{

  lazy val hadoopOptionsMap: Map[String, String] =
    settings.hadoopConfigs.entrySet().map{ e => (e.getKey, settings.hadoopConfigs.getString(e.getKey)) }.toMap

  lazy val sparkSessionOptionsMap: Map[String, String] =
    settings.sparkConfigs.entrySet().map(e => (e.getKey, settings.sparkConfigs.getString(e.getKey))).toMap

  def sparkConfigsToCMLString: String =
    sparkSessionOptionsMap.map{ case (k, v) => if(k.startsWith("spark")) "-conf " + k + "=" + v else k + "=" + v }.mkString(" ")

  def hadoopOptionsToCLMString: String = hadoopOptionsMap.map( e => "-D" + e._1 + "=" + e._2).mkString(" ")

  override def toCMLString: String = sparkConfigsToCMLString + " --driver-java-options '" + hadoopOptionsToCLMString + "'"

}
