package pl.arapso.examples.spark.s3.copy

import java.net.URI
import java.nio.file.Paths

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext


object CopyLocalToS3 extends App {

  val logger = Logger("CopyS3ToLocal")

  val filesToCopy = LocalStore.ls(args(0))

  logger.info(s"Avail files $filesToCopy")

  val sc = SparkContext.getOrCreate()

  def copy(localFilePath: URI, targetPath: URI): Unit = {
    val reader = LocalStore.read(localFilePath)
    val writer = S3Store.write(targetPath)
    writer(reader())
  }

  sc.parallelize(filesToCopy).distinct().mapPartitions {
    _.map { srcFile =>
      copy(
        srcFile,
        new URI(s"${args(1)}/${Paths.get(srcFile).getFileName.toString}")
      )
      srcFile
    }
  }.collect().toSet
  sc.stop()
}
