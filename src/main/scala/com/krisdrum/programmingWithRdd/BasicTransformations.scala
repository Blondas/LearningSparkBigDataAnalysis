package com.krisdrum.programmingWithRdd

import org.apache.spark.SparkContext

class BasicTransformations(sc: SparkContext) {
  // map
  val input = sc.parallelize(List(1,2,3,4))
  val result = input map(x => x * x)
  println(result.collect().mkString(","))

  // flatMap
  val lines = sc.parallelize(List("hello world", "hello"))
  val words = lines.flatMap(_.split(" "))
  words first

  // .distinct <- expensive
  // .union, .intersection, .subtract

  // aggregate:
  val result2 = input.aggregate(0 -> 0)(
    (acc, value) => (acc._1 + value, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )
  val avg = result2._1 / result2._2.toDouble
}
