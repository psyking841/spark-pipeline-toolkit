package com.pan.spark.batch.utils.cloudstorage

import java.net.URI
import java.nio.file.Path

import com.pan.spark.batch.utils.Utils
import org.joda.time.DateTime

/**
  * A general class to handle input paths generation for the Spark app
  */
class SinkUriBuilder extends UriBuilderBase {
  var startDate: DateTime = _

  /**
    * Get a single uri based on start date;
    * for example, if you have start date: 2018-09-10T00:00:00, and layout is "hourly"
    * then you will have a URI: schema://bucket/full/path/year=2018/month=09/day=10/hour=00.
    * Otherwise, if startdate is None, return prefix path, e.g. schema://bucket/full/path/;
    * this is generally happen when one is trying to using Spark partitioning for partitioning the
    * data by date.
    * @return
    */
  def getPathUri(): URI = {
    layout match {
      case "daily" | "hourly" => {
        val fullPath: Path = Utils.formatFullPath(absPrefix, startDate, layout)
        new URI(schema, bucket, fullPath.toString, null)
      }
      case _ => super.getPrefixPathUri()
    }
  }
}

object SinkUriBuilder {

  def builder(): Builder = new Builder

  class Builder {
    private var pathUri: SinkUriBuilder = new SinkUriBuilder

    def withSchema(schema: String): Builder = {
      pathUri.schema = schema
      this
    }

    def withBucket(bucket: String): Builder = {
      pathUri.bucket = bucket
      this
    }

    def withPathPrefix(PathPrefix: String): Builder = {
      pathUri.pathPrefix = PathPrefix
      this
    }

    def withStartDate(startDate: DateTime): Builder = {
      pathUri.startDate = startDate
      this
    }

    def withLayout(layout: String): Builder = {
      pathUri.layout = layout
      this
    }

    def build(): SinkUriBuilder = {
      return pathUri
    }
  }
}
