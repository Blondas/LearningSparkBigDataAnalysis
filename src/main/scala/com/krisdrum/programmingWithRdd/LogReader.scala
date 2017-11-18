package com.krisdrum.programmingWithRdd

import org.apache.spark.{SparkConf, SparkContext}

object LogReader extends App {
        val conf = new SparkConf().setMaster("local").setAppName("SimpleSparkApp1")
        val sc = new SparkContext(conf)

        val inputRRD = sc.textFile("log.txt")

        // transformations:
        val errorRDD = inputRRD.filter(_.contains("error"))
        val warningRDD = inputRRD.filter(_.contains("error"))
        val badLinesRDD = errorRDD union warningRDD

        // persist RDD in memory:
        badLinesRDD.persist

        // actions:
        println(s"Input had ${badLinesRDD.count} concerning lines")
        println("Here are 10 examples:")
        badLinesRDD take 10 foreach println
}