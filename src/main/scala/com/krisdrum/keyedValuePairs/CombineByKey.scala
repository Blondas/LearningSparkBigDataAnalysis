package com.krisdrum.keyedValuePairs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class ScoreCollector(noOfScores: Int, sumOfScores: Double) {
  def avg = noOfScores / sumOfScores
}

object CombineByKey extends App {

  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)

  val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))

  val wilmaAndFredScores = sc.parallelize(initialScores).cache

  val mergeCombiners = (score: Double) => ScoreCollector(1, score)

  val scoreCombiner = (col: ScoreCollector, score: Double) =>
    ScoreCollector(col.noOfScores + 1,  col.sumOfScores + score)

  val scoreMerger = (col1: ScoreCollector, col2: ScoreCollector) =>
    col1.copy(col1.noOfScores + col2.noOfScores, col2.sumOfScores + col2.sumOfScores)

  val scores: RDD[(String, ScoreCollector)] = wilmaAndFredScores.combineByKey(
    mergeCombiners,
    scoreCombiner,
    scoreMerger
  )
}
