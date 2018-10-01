package com.pan.spark.batch.app

import com.pan.spark.batch.Params

import scala.collection.JavaConversions._

/**
  * This class converts settings into internal maps for configuring Spark
  * @param settings
  */
class AppParams(settings: BatchAppSettings) extends Params{

  lazy val fsOptionsMap: Map[String, String] =
    settings.fsConfigs.entrySet().map{e => (e.getKey, settings.fsConfigs.getString(e.getKey))}.toMap

  val sparkSessionOptionsMap: Map[String, String] = {
    var res: Map[String, String] = Map()
    res ++= Map("master" -> settings.sparkConfigs.getString("master"))
    res ++ (for( e <- settings.sparkConfigs.entrySet(); if e.getKey != "master" )
              yield ("spark." + e.getKey, settings.sparkConfigs.getString(e.getKey))).toMap
  }

  def sparkConfigsToCMLString: String = {
    sparkSessionOptionsMap
      .map{ case (k, v) => if(k.startsWith("spark")) "-conf " + k + "=" + v else k + "=" + v }.mkString(" ")
  }

  def hadoopOptionsToCLMString: String = {
    fsOptionsMap.map( e => "-Dfs." + e._1 + "=" + e._2).mkString(" ")
  }

  override def toCMLString: String = {
    sparkConfigsToCMLString + " --driver-java-options '" + hadoopOptionsToCLMString + "'"
  }

}
