package com.krisdrum.loadingSavingData

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class SequenceFile(sc: SparkContext) {
  val inFile: String = ???
  val outFIle: String = ???

  // loading SequenceFile
  val data: RDD[(String, Int)] = sc.sequenceFile(
    path = inFile,
    keyClass = classOf[Text],
    valueClass =  classOf[IntWritable]
  ).map{ case (x, y) => (x.toString, y.get)}

  // or:
  val data2: RDD[(String, Int)] = sc.sequenceFile[String, Int](inFile)


  data.saveAsSequenceFile(outFIle)
}
