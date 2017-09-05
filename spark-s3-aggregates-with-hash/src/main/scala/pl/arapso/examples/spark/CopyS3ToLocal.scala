package pl.arapso.examples.spark

import java.net.URI
import java.nio.file.Paths

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext


object CopyS3ToLocal extends App {

  val logger = Logger("CopyFromS3ToLocal")

  val filesToCopy = S3Store.ls(new URI(args(0)))

  logger.info(s"Avail files $filesToCopy")

  val sc = SparkContext.getOrCreate()

  def copy(localFilePath: URI, targetPath: URI): Unit = {
    val reader = S3Store.read(localFilePath)
    val writer = LocalStore.write(targetPath)
    writer(reader())
  }

  val filesRDD = sc.parallelize(filesToCopy)

  val filteredRDD =
    if(args.length == 3) filesRDD.filter(x => Paths.get(x.getPath).getFileName.toString.startsWith(args(2)))
    else filesRDD

    filteredRDD.
    distinct().
    mapPartitions {
    _.map { srcFile =>
      copy(
        srcFile,
        new URI(s"${args(1)}/${Paths.get(srcFile.getPath).getFileName.toString}")
      )
      srcFile
    }
  }.collect().toSet
  sc.stop()
}
