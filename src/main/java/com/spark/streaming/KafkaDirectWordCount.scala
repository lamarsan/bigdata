package com.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaDirectWordCount {
  def main(args : Array[String]):Unit ={

    if(args.length != 2) {
      System.err.println("Usage:KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }
    val Array(brokers,topics) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"-> brokers)

    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)

    messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
