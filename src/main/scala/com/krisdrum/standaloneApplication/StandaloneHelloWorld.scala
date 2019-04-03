package com.krisdrum.standaloneApplication

import org.apache.spark.{SparkConf, SparkContext}

object StandaloneHelloWorld extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)
  val lines = sc.textFile("src/main/resources/vulgata.txt")
  println(lines.count)
}
