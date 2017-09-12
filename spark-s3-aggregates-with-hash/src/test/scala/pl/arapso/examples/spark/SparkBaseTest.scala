package pl.arapso.examples.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkBaseTest extends BeforeAndAfterAll {
  this: Suite =>

  private val Master = "local[*]"

  private val AppName = s"spark-test-${getClass.getSimpleName}"

  protected implicit lazy val spark: SparkSession =
    sessionBuilder().getOrCreate()

  protected lazy val sc: SparkContext = spark.sparkContext

  protected def sessionBuilder(): SparkSession.Builder = {
    SparkSession.builder.
      master(Master).
      appName(AppName).
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      //config("spark.kryo.registrator", kryoRegistratorClassName).
      config("spark.ui.enabled", "false").
      config("spark.ui.showConsoleProgress", "false").
      config("spark.sql.shuffle.partitions", 1).
      config("spark.default.parallelism", 1)
  }

  override protected def afterAll() {
    println("Closing spark")
    spark.close()
  }

}
