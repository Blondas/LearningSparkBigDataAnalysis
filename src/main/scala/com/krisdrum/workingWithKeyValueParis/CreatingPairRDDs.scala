package com.krisdrum.workingWithKeyValueParis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class CreatingPairRDDs(sc: SparkContext) {
  val lines: RDD[String] = sc.parallelize(List("hello world", "hello"))
  val pairs: RDD[(String, String)] = lines.map(x => x.split(" ")(0) -> x)
  val intPairs = sc.parallelize(List(1 -> 2, 3 -> 4, 3 -> 6))


  val reduceByKey: RDD[(Int, Int)] = intPairs.reduceByKey((x, y) => x + y )
  val groupedBykey: RDD[(Int, Iterable[Int])] = intPairs.groupByKey()
//  val combinedByKey = ???
  val mappedValues = intPairs.mapValues(_+1)
  val flatMappedValues: RDD[(Int, Int)] = intPairs.flatMapValues(e => e to 5)
  val keys = intPairs.keys
  val values = intPairs.values
  val sortedByKey = intPairs.sortByKey()


  // transformations of two pair RDDs
  val other: RDD[(Int, Int)] = sc.parallelize(List(3 -> 9))

  val substractedByKey: RDD[(Int, Int)] = intPairs subtractByKey other
  val joined: RDD[(Int, (Int, Int))] = intPairs join other
  val rightOuterJoined = intPairs rightOuterJoin other
  val leftOuterJoined = intPairs leftOuterJoin other
  val cogroupped = intPairs cogroup other


  // pery key average:
  val perKyeyAverage: RDD[(Int, (Int, Int))] =
    intPairs.mapValues(_ -> 1).reduceByKey((x, y)=> (x._1 + y._1, x._2 + y._2))


  // implementation of distributed word count problem:
  val file = """C:\Users\nkrzy\Scala\Spark\LearningSparkBigDataAnalysis\src\main\resources\vulgata.txt"""
  val input: RDD[String] = sc.textFile(file)
  val words = input.flatMap(_.split(" "))
  val distributetWordCount = words.map(_->1).reduceByKey((x,y) => x+y)
}

object TestApp extends App {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("TestApp")
  val sc = new SparkContext(conf)

  val rddPair = new CreatingPairRDDs(sc)

  val reducedByKey = rddPair.reduceByKey.collect.toList
  val groupedByKey = rddPair.groupedBykey.collect.toList
  val mappedValues = rddPair.mappedValues.collect.toList
  val flatMappedValues = rddPair.flatMappedValues.collect.toList
  val keys = rddPair.keys.collect.toList
  val values = rddPair.values.collect.toList
  val sortedByKey = rddPair.sortedByKey.collect.toList

  val substractedByKey = rddPair.substractedByKey.collect.toList
  val joined = rddPair.joined.collect.toList
  val rightOuterJoined = rddPair.rightOuterJoined.collect.toList
  val leftOuterJoined = rddPair.leftOuterJoined.collect.toList
  val cogroupped: List[(Int, (Iterable[Int], Iterable[Int]))] = rddPair.cogroupped.collect.toList

  val perKeyAverage = rddPair.perKyeyAverage.collect.toList

  val distrWrdCnt = rddPair.distributetWordCount.collect.toList

  println(
//    "rbk_" + reducedByKey,
//    "gbk_" +groupedByKey,
//    "mv_" + mappedValues,
//    "fmv_" + flatMappedValues,
//    "k_" + keys,
//    "v_" + values,
//    "sbk_" + sortedByKey,
//
//    "sbtbk_" + substractedByKey,
//    "j_" + joined,
//    "roj_" + rightOuterJoined,
//    "loj_" + leftOuterJoined,
    "cg_" + cogroupped

//    "pka_" + perKeyAverage
//      "dwc_" + distrWrdCnt.take(10)
  )
}

