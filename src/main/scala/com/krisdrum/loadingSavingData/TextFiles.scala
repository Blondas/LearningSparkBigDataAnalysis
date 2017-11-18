package com.krisdrum.loadingSavingData

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TextFiles(sc: SparkContext) {
  // pojedynczy plik, lub caly katalog do jednego RDD
  sc.textFile("file: ///home/holden/repos/spark/README.md")

  // katalog z plikami do roznych RDD:
  val input: RDD[(String, String)] = sc.wholeTextFiles("file://home/holden/salesFiles")
  val result = input.mapValues{ v =>
    val nums: Array[Double] = v.split(" ").map(_.toDouble)
    nums.sum / nums.size.toDouble
  }

  result.saveAsTextFile("outputFile")
}
