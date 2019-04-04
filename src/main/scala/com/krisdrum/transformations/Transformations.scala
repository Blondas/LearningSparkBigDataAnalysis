package com.krisdrum.transformations

import org.apache.spark.{SparkConf, SparkContext}

object Transformations extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)

  val inputRdd = sc.textFile("src/main/resources/logs")

  val errorsRDD = inputRdd.filter(_.contains("error"))
  val warningsRDD = inputRdd.filter(_.contains("error"))
  val badLinesRDD = errorsRDD union warningsRDD

  println(badLinesRDD.collect().toSeq)
}
