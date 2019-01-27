package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

object FlumePullWordCount {
  def main(args: Array[String]): Unit = {

    //    if(args.length != 2) {
    //      System.err.println("Usage: FlumePushWordCount <hostname> <port>")
    //      System.exit(1)
    //    }

    //    val Array(hostname , post) = args

    val sparkConf = new SparkConf()
      //.setMaster("local[2]").setAppName("FlumePushWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val flumeStream = FlumeUtils.createPollingStream(ssc,"192.168.110.128",41414)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
