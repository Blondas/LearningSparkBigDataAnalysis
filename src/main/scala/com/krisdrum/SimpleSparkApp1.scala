//package com.krisdrum
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//
//object SimpleSparkApp1 extends App{
//  val inputFile = args(0)
//
//  val conf = new SparkConf().setMaster("local").setAppName("SimpleSparkApp1")
//  val sc = new SparkContext(conf)
//
//  val input = sc.textFile(inputFile)
//  val lines: RDD[String] = input.map(line => line.toLowerCase)
//  val filter: String => Boolean = s => s.contains("deus")
//
//  val ret: RDD[String] = lines.filter(filter)
//
//  println(ret)
//}
//
//
