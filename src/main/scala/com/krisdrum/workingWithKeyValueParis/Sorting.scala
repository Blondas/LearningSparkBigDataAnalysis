package com.krisdrum.workingWithKeyValueParis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Sorting(sc: SparkContext) {
  val input: RDD[(Int, String)] = ???

  implicit val sortIntegersByString = new Ordering[Int] {
    override def compare(a: Int, b: Int) =
      a.toString.compare(b.toString)
  }

  input.sortByKey()
}
