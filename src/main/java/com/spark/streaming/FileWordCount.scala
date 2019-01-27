package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {

    def main(args : Array[String]) : Unit = {

      val sparkConf = new SparkConf().setMaster("192.168.110.128[2]").setAppName("FileWordCount")
      val ssc = new StreamingContext(sparkConf,Seconds(5))

      val lines = ssc.textFileStream("file:///root/data/")
      val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
      result.print()

      ssc.start()
      ssc.awaitTermination()
    }
}
