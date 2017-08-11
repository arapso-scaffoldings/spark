package pl.arapso.examples.spark

import java.io._
import java.net.URI

import com.amazonaws.services.s3.AmazonS3Client

object LocalStore extends Store {

  @transient
  private lazy val client = new AmazonS3Client()

  def read(src: URI): Producer = () => {
    val file = new File(src)
    new BufferedInputStream(new FileInputStream(file))
  }

  def write(targetPath: URI): Consumer = {
    targetPath.getPath.split('/').reverse.tail.foldRight[String](".")((folder: String, path: String) => {
      val folderPathToCreate = s"$path/$folder"
      val parentFolder = new File(folderPathToCreate)
      if (!parentFolder.exists()) {
        parentFolder.mkdir()
      }
      folderPathToCreate
    })

    (is) => {
      try {
        val target = new BufferedOutputStream(new FileOutputStream(targetPath.getPath))
        val buff = new Array[Byte](DefaultBufferSize)
        Iterator.continually(is.read(buff)).takeWhile(_ != -1).foreach { c =>
          target.write(buff, 0, c)
        }
        target.close()
      } finally is.close()
    }
  }

  def ls(pathToUpload: String): List[URI] = {
    val file = new File(pathToUpload)
    file.listFiles().filter(!_.isHidden).filter(_.getName != "_SUCCESS").toList.map(_.toURI)
  }

}
