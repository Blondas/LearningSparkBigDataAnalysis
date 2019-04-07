package com.krisdrum.keyedValuePairs.commonTransformationsAndActions

import org.apache.spark.{SparkConf, SparkContext}

object MapFunction extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)

  val input = sc.parallelize(Seq(1,2,3,4,5))
  val result = input map (x => x * x)

  println(result.collect.mkString(","))
}
