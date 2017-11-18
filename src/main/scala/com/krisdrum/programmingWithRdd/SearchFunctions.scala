package com.krisdrum.programmingWithRdd

import org.apache.spark.rdd.RDD

class SearchFunctions(val query: String) {
  def isMatch(s: String) = s contains query

  def getMatchesFunctionReference(rdd: RDD[String]) = rdd.map(isMatch)

  def getMatechesFieldReference(rdd: RDD[String]) = rdd.map(_.split(query))

  def getMatchesNoReference(rdd: RDD[String]) = {
    val query_ = query
    rdd.map(_.split(query_))
  }
}
