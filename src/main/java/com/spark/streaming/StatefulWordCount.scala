package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args : Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //如果使用了stateful的算子，必须要设置checkpoint
    //在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("192.168.110.128",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues:Seq[Int],preValues: Option[Int]):Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }
}
