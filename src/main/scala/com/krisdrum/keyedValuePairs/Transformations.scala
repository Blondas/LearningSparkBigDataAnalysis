package com.krisdrum.keyedValuePairs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformations extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)
  val lines: RDD[String] = sc.textFile("src/main/resources/vulgata.txt")

  val words = lines.flatMap(_.split(" ")).filter(! _.contains("|"))

  // reduceByKey
  val wCount = words.map(w => w -> 1).reduceByKey((x, y) => x + y)
  println( wCount take 10 toSeq)


}
