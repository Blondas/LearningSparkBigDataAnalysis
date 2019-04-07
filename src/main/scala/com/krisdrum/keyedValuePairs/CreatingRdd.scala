package com.krisdrum.keyedValuePairs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreatingRdd extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)
  val lines = sc.textFile("src/main/resources/vulgata.txt")

  val pairs: RDD[(String, String)] = lines.map(x => (x.split(" ")(0) , x) )

  println(pairs take 5 toSeq)
}
