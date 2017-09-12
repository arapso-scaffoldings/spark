package pl.arapso.examples.spark.datasets

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import pl.arapso.examples.spark.SparkBaseTest
import pl.arapso.examples.spark.datasets.DatasetsProcessing.{Aggregate, ProcessedRequest, _}

class DatasetsProcessingTest
  extends FlatSpec with Matchers with BeforeAndAfter with SparkBaseTest {

  behavior of "Datasets processing examples"

  it should "join two datasets" in {
    val requests = sc.parallelize(Seq(
      ProcessedRequest("123", Some("wp.pl"), Some("http://www.wp.pl/page1?test=1")),
      ProcessedRequest("124", Some("nobid.com"), Some("www.nobid.com/sample"))
    ))

    val eventsSource =
      """
        |{"event": "WIN", "bidId": "123:321", "campaignId": "1", "creativeId": "1", "cost": 12}
        |{"event": "WIN", "bidId": "125:321", "campaignId": "1", "creativeId": "2", "cost": 31}
      """.stripMargin.split("\n").toList

    val events = sc.parallelize(eventsSource)
    val result = parseAndJoin(requests, events).collect()

    result should {
      have length 1
      contain theSameElementsAs Seq(Aggregate("1", "1", "wp.pl", 1, 0, 12))
    }
  }

  it should "join multiple events to one request" in {
    val requests = sc.parallelize(Seq(
      ProcessedRequest("123", Some("wp.pl"), None),
      ProcessedRequest("124", Some("wp.pl"), None),
      ProcessedRequest("125", Some("onet.pl"), None)
    ))
    val eventsSource =
      """
        |{"event": "WIN", "bidId": "123:321", "campaignId": "1", "creativeId": "1", "cost": 12}
        |{"event": "WIN", "bidId": "124:321", "campaignId": "1", "creativeId": "1", "cost": 31}
        |{"event": "CLICK", "bidId": "123:321", "campaignId": "1", "creativeId": "1"}
        |{"event": "WIN", "bidId": "125:321", "campaignId": "1", "creativeId": "2", "cost": 12}
        |{"event": "CLICK", "bidId": "125:321", "campaignId": "1", "creativeId": "2"}
      """.stripMargin.split("\n").toList

    val result = parseAndJoin(requests, sc.parallelize(eventsSource)).collect()

    result should {
      have length 1
      contain theSameElementsAs Seq(
        Aggregate("1", "1", "wp.pl", 2, 1, 43),
        Aggregate("1", "2", "onet.pl", 1, 1, 12)
      )
    }
  }

  it should "join multiple events to same request" in {
    val requests = sc.parallelize(Seq(
      ProcessedRequest("123", Some("roq.ad"), None)
    ))

    val eventsSource =
      """
        |{"event": "WIN", "bidId": "123:123", "campaignId": "1", "creativeId": "1", "cost": 12}
        |{"event": "WIN", "bidId": "123:124", "campaignId": "2", "creativeId": "2", "cost": 22}
      """.stripMargin.split("\n").toList

    val result = parseAndJoin(requests, sc.parallelize(eventsSource)).collect()

    result should {
      have length 1
      contain theSameElementsAs Seq(
        Aggregate("1", "1", "roq.ad", 1, 0, 12),
        Aggregate("2", "2", "roq.ad", 1, 0, 22)
      )
    }

  }

  behavior of "event parser"

  it should "read and parse events" in {
    val eventsSource = """
      |{"event": "WIN", "bidId": "123", "campaignId": "1", "creativeId": "1", "cost": 12}
    """.stripMargin.split("\n").toList

    val result = Event.toDF(sc.parallelize(eventsSource)).collect()

    result should {
      have length 1
      contain theSameElementsAs Seq(Event("123", "1", "1", 12))
    }
  }

  it should "split bidId if consists of parts separated with colon" in {
    val eventsSource =
      """
        |{"event": "WIN", "bidId": "123:12", "campaignId": "1", "creativeId": "1", "cost": 12}
        |{"event": "WIN", "bidId": "124:414", "campaignId": "1", "creativeId": "2", "cost": 22}
      """.stripMargin.split("\n").toList

    val result = Event.toDF(sc.parallelize(eventsSource)).collect()

    result should {
      have length 2
      contain theSameElementsAs Seq(
        Event("123", "1", "1", 12), Event("124", "1", "2", 22)
      )
    }
  }

  it should "contains only distinct events" in {
    val eventsSource =
      """
        |{"event": "WIN", "bidId": "123:123", "campaignId": "1", "creativeId": "1", "cost": 12}
        |{"event": "WIN", "bidId": "123:123", "campaignId": "1", "creativeId": "1", "cost": 12}
      """.stripMargin.split("\n").toList
    val events = sc.parallelize(eventsSource)
    val result = Event.toDF(events).collect()

    result should {
      have length 1
      contain theSameElementsAs Seq(
        Event("123", "1", "1", 12)
      )
    }
  }

}
