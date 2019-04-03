package com.krisdrum.wordCount

import com.krisdrum.standaloneApplication.StandaloneHelloWorld.getClass
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App{
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)
  val lines = sc.textFile(input)

  val result = lines
    .flatMap(_.split(" "))
    .map(w => w -> 1)
    .reduceByKey{case (x,y) => x + y}
    .sortBy(_._2, false)
    .take(10)
    .toSeq

  println(result)
}
