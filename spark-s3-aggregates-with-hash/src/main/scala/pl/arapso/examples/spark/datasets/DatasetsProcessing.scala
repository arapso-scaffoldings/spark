package pl.arapso.examples.spark.datasets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, substring_index, sum, when}
import org.apache.spark.sql.{Dataset, SparkSession}

object DatasetsProcessing extends App {
  case class Request(bidId: String, domain: String)

  case class Event(
    sspBidId: String,
    campaignId: String,
    creativeId: String,
    cost: Long
  )

  case object Event {

    def toDF(eventsSource: RDD[String])(implicit sparkSession: SparkSession): Dataset[Event] = {
      import sparkSession.implicits._

      sparkSession.read.json(eventsSource).
        distinct().
        withColumn("sspBidId", substring_index(col("bidId"), ":", 1)).
        withColumnRenamed("bidId", "eventBidId").
        withColumn("imps", when('event.equalTo("WIN"), 1).otherwise(0)).
        withColumn("clicks", when('event.equalTo("CLICK"), 1).otherwise(0)).as[Event]
    }

  }

  case class Aggregate(
    campaignId: String,
    creativeId: String,
    domain: String,
    imps: Long,
    clicks: Long,
    cost: Long
  )

  def parseAndJoin(requests: RDD[Request], eventsSource: RDD[String])(implicit spark: SparkSession): Dataset[Aggregate] = {
    import spark.implicits._
    val events = Event.toDF(eventsSource)

    val joinedEvents = requests.toDF().join(events, 'bidId === 'sspBidId)

    joinedEvents.
      groupBy("campaignId", "creativeId", "domain").
      agg(
        sum("imps").alias("imps"),
        sum("clicks").alias("clicks"),
        sum("cost").alias("cost")
      ).
      as[Aggregate]
  }

}
