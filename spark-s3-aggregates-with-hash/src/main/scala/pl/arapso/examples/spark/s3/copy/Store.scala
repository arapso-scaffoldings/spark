package pl.arapso.examples.spark.s3.copy

import java.io.InputStream
import java.net.URI

trait Store {

  type Producer = () => InputStream

  type Consumer = InputStream => Unit

  val DefaultBufferSize: Int = 1024 * 512

  def getBucketAndKey(uri: URI): (String, String) = {
    val bucket = uri.getHost
    val key = uri.getPath.dropWhile(_ == '/')
    (bucket, key)
  }

}
