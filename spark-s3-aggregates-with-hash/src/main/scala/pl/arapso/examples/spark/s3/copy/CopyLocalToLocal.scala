package pl.arapso.examples.spark.s3.copy

import java.io._
import java.net.URI

object CopyLocalToLocal {

  def copy(localFilePath: URI, targetPath: URI): Unit = {
    val reader = LocalStore.read(localFilePath)
    val writer = LocalStore.write(targetPath)
    writer(reader())
  }

}

object LocalTest extends App {

  CopyLocalToLocal.copy(new File(".","input.txt").toURI, new File(".","output.txt").toURI)

}
