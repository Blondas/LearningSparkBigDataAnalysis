package com.krisdrum.keyedValuePairs.pageRank

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {
  val conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)

  val links: RDD[(String, Seq[String])] = sc.objectFile[(String, Seq[String])]("links")
    .partitionBy(new HashPartitioner(100))
    .persist

  var ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)

  (1 to 10) foreach { x =>
    val contributions = links.join(ranks).flatMap{
      case (pageId, (pageLinks, rank)) => pageLinks.map(dest => (dest, rank / pageLinks.size))
    }

    ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
  }
}
