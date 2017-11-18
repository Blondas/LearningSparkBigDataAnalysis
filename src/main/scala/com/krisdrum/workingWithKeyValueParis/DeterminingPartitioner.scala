package com.krisdrum.workingWithKeyValueParis

import org.apache.spark.{HashPartitioner, SparkContext}

class DeterminingPartitioner(sc: SparkContext) {
  val pairs = sc.parallelize(List(1->2, 2->2, 3->3))
  pairs.partitioner // zwraca None
  val partitioned = pairs.partitionBy(new HashPartitioner(2))
  partitioned.partitioner // Some(HashPartitioner@6879)
}
