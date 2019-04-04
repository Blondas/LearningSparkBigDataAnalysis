package com.krisdrum.commonTransformationsAndActions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapFunction extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)

  val input = sc.parallelize(Seq("hello", "world", "hi"))
  val result: RDD[String] = input flatMap (_ split " ")

  println (result.collect.toSeq)
}
