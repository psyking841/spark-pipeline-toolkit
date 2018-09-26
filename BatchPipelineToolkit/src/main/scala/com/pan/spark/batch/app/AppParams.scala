package com.pan.spark.batch.app

import com.pan.spark.batch.Params

class AppParams(settings: BatchAppSettings) extends Params{

  val ibmServiceName =
    if(settings.defaultConfigs.hasPath("ibmServiceName")) settings.defaultConfigs.getString("ibmServiceName")
    else "defaultServiceName"

  val MAPPINGS: Map[String, String] = Map(
    "awsSecretKey" -> "fs.s3a.awsSecretAccessKey",
    "awsKeyId" -> "fs.s3a.awsAccessKeyId",
    "ibmCosSecretKey" -> ("fs.cos" + ibmServiceName + "secret.key"),
    "ibmCosAccessKey" -> ("fs.cos." + ibmServiceName + ".access.key"),
    "endpoint" -> ("fs.cos." + ibmServiceName + ".endpoint"),
    "serviceId" -> ("fs.cos." + ibmServiceName + ".iam.service.id"))

  val optionsMap: Map[String, String] = {
    MAPPINGS.keys.flatMap({
      k => { if (settings.defaultConfigs.hasPath(k)) Some(k, settings.defaultConfigs.getString(k)) else None }
    }).toMap
  }

  val sparkOptionsMap: Map[String, String] = {
    optionsMap.map( e => MAPPINGS(e._1) -> e._2 )
  }

  def asJavaOptions(): String = {
    optionsMap.map( e => "-D" + e._1 + "=" + e._2).mkString(" ")
  }

}
