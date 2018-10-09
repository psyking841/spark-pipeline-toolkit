package com.span.spark.batch.utils.cloudstorage

import java.net.URI
import java.nio.file.Path

import com.span.spark.batch.utils.Utils
import org.joda.time.DateTime

/**
  * A general class to handle input paths generation for the Spark app
  */
class SourceUriBuilder extends UriBuilderBase {
  var startDate: DateTime = _
  var endDate: DateTime = _

  /**
    * Get a sequence of uri based on start date (inclusive and end date (exclusive);
    * for example, if you have start date: 2018-09-10T00:00:00, and end date: 2018-09-10T02:00:00 and layout is "hourly"
    * then you will have a set of URIs:
    *
    * { schema://bucket/full/path/year=2018/month=09/day=10/hour=00,
    * schema://bucket/full/path/year=2018/month=09/day=10/hour=01}
    *
    * @return
    */
  def getPathUriSet(): Seq[URI] = {
    val allFullPath: Seq[Path] = Utils.getFullPaths(absPrefix, startDate, endDate, layout)
    allFullPath.map(p => new URI(schema, bucket, p.toString, null))
  }
}

object SourceUriBuilder {

  def builder(): Builder = new Builder

  class Builder {
    private var pathUri: SourceUriBuilder = new SourceUriBuilder

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

    def withEndDate(endDate: DateTime): Builder = {
      pathUri.endDate = endDate
      this
    }

    def withLayout(layout: String): Builder = {
      pathUri.layout = layout
      this
    }

    def build(): SourceUriBuilder = {
      pathUri
    }
  }
}
