package com.krisdrum.workingWithKeyValueParis

import org.apache.spark.SparkContext

class TuningParalellismLevel(sc: SparkContext) {
  val data = Seq(("a", 3), ("b", 4), ("a", 1))
  sc.parallelize(data).reduceByKey((x,y)=>x+y, 3)
}
