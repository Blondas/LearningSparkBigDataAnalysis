package com.krisdrum.keyedValuePairs.commonTransformationsAndActions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate extends App {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)

  val input = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9))

  val zero: (Int, Int) = 0 -> 0

  def combine(acc: (Int, Int), value: Int) = (acc._1 + value, acc._2 + 1)

  def reduce(acc1: (Int, Int), acc2: (Int, Int)) = (acc1._1 + acc2._1, acc1._2 + acc2._2)

  val result: (Int, Int) = input.aggregate(zero)(combine, reduce)

  println(result._1 / result._2.toDouble)
}
