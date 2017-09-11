import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SparkExampleTest extends FlatSpec with Matchers with BeforeAndAfter with SparkBaseTest {

  behavior of "SparkSession"

  before {
    println("Start test")
  }

  after {
    println("Test ended")
  }

  it should "verify number of items in rdd" in {

    val result: Seq[String] = spark.sparkContext.parallelize(List("temp")).collect().toList

    result should have length 1
    result should contain theSameElementsAs List("temp")
  }
  
}
