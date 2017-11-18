package com.krisdrum.workingWithKeyValueParis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class CombineByKeyExamples(sc: SparkContext) {
  val input: RDD[(Int, Int)] = sc.parallelize(List(1 -> 2, 3 -> 4, 3 -> 6))

  val result: RDD[(Int, Int)] = input.combineByKey(
    (v) => (v,1), // createCombiner
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // mergeValues
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // mergeCombiners
  ).map { case (key, value) => (key, value._1 / value._2)}

    def printResult() = result.collectAsMap.map(println(_))



}
