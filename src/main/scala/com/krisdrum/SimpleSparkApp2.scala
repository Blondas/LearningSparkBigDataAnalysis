package com.krisdrum
import org.apache.spark.{SparkConf, SparkContext}

object SimpleSparkApp2 extends App {
    val inputFile = """C:\Users\nkrzy\Scala\Spark\LearningSparkBigDataAnalysis\src\main\resources\vulgata.txt"""
    val outputFile = """C:\Users\nkrzy\Scala\Spark\LearningSparkBigDataAnalysis\src\main\resources\vulgataOut.txt"""

    val conf = new SparkConf().setMaster("local").setAppName("SimpleSparkApp1")
    val sc = new SparkContext(conf)

    def wordSplit(string: String) = string.split("""\W+""")
    val input = sc.textFile(inputFile)

    val wordCount = input
      .flatMap(wordSplit)
      .map(word => word -> 1)
      .reduceByKey(_+_)

    println(wordCount)

    wordCount.saveAsTextFile(outputFile)
}


