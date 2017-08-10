package pl.arapso.examples.spark 

import org.apache.spark.{SparkConf, SparkContext}

object SparkGrepScala extends App {

    val sc = new SparkContext(new SparkConf().setAppName("Grep"))
    val keyWords = args(2).split(",")
    val input = sc.textFile(args(0))
    val filteredLines = input.filter(line => keyWords.exists( word => line.contains(word)))
    filteredLines.saveAsTextFile(args(1))
}
