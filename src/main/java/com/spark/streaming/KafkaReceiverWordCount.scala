package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka的方式一
  */
object KafkaReceiverWordCount {

  def main(args : Array[String]):Unit ={

    if(args.length != 4) {
      System.err.println("Usage:KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum,group,topics,numThreads) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

    messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
