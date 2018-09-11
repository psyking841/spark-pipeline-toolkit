package com.pan.spark.batch.utils.cloudstorage

import java.net.URI
import java.nio.file.{Path, Paths}

class UriBuilderBase {
  var schema: String = _
  var bucket: String = _
  var pathPrefix: String = _
  var layout: String = _
  val absPrefix: Path = Paths.get("/", pathPrefix) //make it an absolute path

  /**
    * Get a single uri that only represents the path prefix;
    * for e.g. /bucket/pathPrefix/
    * @return
    */
  def getPrefixPathUri(): URI = {
    new URI(schema, bucket, absPrefix.toString, null)
  }
}
