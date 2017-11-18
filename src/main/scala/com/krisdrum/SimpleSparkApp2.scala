package com.krisdrum
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SimpleSparkApp2 extends App {

        val inputFile = args(0)
        val outputFolder = args(1)

        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("SimpleSparkApp1")
        val sc = new SparkContext(conf)

        def wordSplit(string: String) = string.split("""\W+""")

        val input: RDD[String] = sc.textFile(inputFile)

        val wordCount: RDD[(String, Int)] = input
          .flatMap(wordSplit)
          .map(word => word -> 1)
          .reduceByKey(_ + _)

        wordCount foreach println
        wordCount.saveAsTextFile(outputFolder)
}


