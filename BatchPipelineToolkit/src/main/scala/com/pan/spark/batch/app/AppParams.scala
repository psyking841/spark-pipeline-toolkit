package com.pan.spark.batch.app

import com.pan.spark.batch.Params

import scala.collection.JavaConversions._

/**
  * This class converts settings into internal maps for configuring Spark
  * @param settings
  */
class AppParams(settings: BatchAppSettings) extends Params{

//  val ibmServiceName: String =
//    if(settings.defaultConfigs.hasPath("ibmServiceName")) settings.defaultConfigs.getString("ibmServiceName")
//    else "defaultServiceName"
//
//  val MAPPINGS: Map[String, String] = Map(
//    "awsSecretKey" -> "fs.s3a.awsSecretAccessKey",
//    "awsKeyId" -> "fs.s3a.awsAccessKeyId",
//    "ibmCosSecretKey" -> ("fs.cos" + ibmServiceName + "secret.key"),
//    "ibmCosAccessKey" -> ("fs.cos." + ibmServiceName + ".access.key"),
//    "endpoint" -> ("fs.cos." + ibmServiceName + ".endpoint"),
//    "serviceId" -> ("fs.cos." + ibmServiceName + ".iam.service.id"))

  lazy val fsOptionsMap: Map[String, String] = {
    settings.fsConfigs.entrySet().map{e => (e.getKey, settings.fsConfigs.getString(e.getKey))}.toMap
//    MAPPINGS.keys.flatMap({
//      k => { if (settings.defaultConfigs.hasPath(k)) Some(k, settings.defaultConfigs.getString(k)) else None }
//    }).toMap
  }

  val sparkSessionOptionsMap: Map[String, String] = {
    var res: Map[String, String] = Map()
    res ++= Map("master" -> settings.sparkConfigs.getString("master"))
    res ++ (for( e <- settings.sparkConfigs.entrySet(); if e.getKey != "master" )
              yield ("spark." + e.getKey, settings.sparkConfigs.getString(e.getKey))).toMap
  }

//  val hadoopOptionsMap: Map[String, String] = {
//    optionsMap.map( e => fsOptionsMap(e._1) -> e._2 )
//  }

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
