package com.krisdrum.passingFunctions

import org.apache.spark.rdd.RDD

class SearchFunctions(query: String) {
  def isMatch(s: String): Boolean = s.contains(query)

  def getMatchersFunctionReference(rdd: RDD[String]): RDD[Boolean] = rdd map isMatch

  def getMatchersFieldReference(rdd: RDD[String]): RDD[Array[String]] = rdd.map(_.split(query))

  def getMatchersNoReference(rdd: RDD[String]): RDD[Array[String]] = {
    val query_ = query
    rdd map (_.split(query_))
  }
}
