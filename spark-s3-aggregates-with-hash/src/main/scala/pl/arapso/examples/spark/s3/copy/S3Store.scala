package pl.arapso.examples.spark.s3.copy

import java.io._
import java.net.URI

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._

object S3Store extends Store {

  import scala.collection.JavaConverters._

  @transient
  private lazy val client = new AmazonS3Client()

  def read(src: URI): Producer = () => {
    val (bucket, key) = getBucketAndKey(src)
    val is: S3ObjectInputStream = client.getObject(bucket, key).getObjectContent
    new BufferedInputStream(is, DefaultBufferSize)
  }

  def write(targetPath: URI): Consumer = {
    (is) => {
      val (bucket, key) = getBucketAndKey(targetPath)

      val partSize = 32 * 1024 * 1024 // 32 megabytes
      val buffer = new Array[Byte](partSize)

      val id = client.initMultipartUpload(bucket, key)

      try {
        val partTags =
          Iterator.continually(is.read(buffer)).takeWhile(_ != -1).zipWithIndex.map {
            case (readBytesCount, idx) =>
              val req = new UploadPartRequest().
                withBucketName(bucket).withKey(key).
                withUploadId(id).
                withPartNumber(idx + 1).
                withInputStream(new ByteArrayInputStream(buffer, 0, readBytesCount)).
                withPartSize(readBytesCount)
              client.uploadPart(req).getPartETag
          }.toList

        client.completeMultipartUpload(bucket, key, id, partTags)
      } catch {
        case e: Exception =>
          client.abortMultipartUpload(bucket, key, id)
          throw e
      } finally {
        is.close()
      }
    }
  }

  def ls(srcPath: URI): Stream[URI] = {
    def loop(prev: ObjectListing): Stream[ObjectListing] = {
      if (prev.isTruncated) prev #:: loop(client.listNextBatchOfObjects(prev))
      else Stream(prev)
    }

    val (bucketName, prefix) = getBucketAndKey(srcPath)

    val temp: ObjectListing = client.listObjects(bucketName, prefix)
    val stream = loop(temp)
    stream.flatMap(_.getObjectSummaries.asScala).map(x => {
      new URI("s3://" + x.getBucketName + "/" + x.getKey)
    })
  }

  private implicit class AmazonS3ClientEx(val c: AmazonS3Client) extends AnyVal {

    def initMultipartUpload(bucket: String, key: String): String = {
      val req = new InitiateMultipartUploadRequest(bucket, key)
      val res = c.initiateMultipartUpload(req)
      res.getUploadId
    }

    def completeMultipartUpload(
      bucket: String, key: String, id: String, parts: List[PartETag]
    ): Unit = {
      c.completeMultipartUpload(new CompleteMultipartUploadRequest(
        bucket, key, id, parts.asJava))
    }

    def abortMultipartUpload(bucket: String, key: String, id: String): Unit = {
      c.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, id))
    }

  }

}

object test extends App {

  val files = S3Store.ls(new URI("s3://roqad-test-de/damian/aggr/2017-08-01/"))
  files.toList.foreach(x => println(x.toString))

}
