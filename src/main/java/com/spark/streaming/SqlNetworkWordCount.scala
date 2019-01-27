package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.socketTextStream("192.168.110.128",6789)

    val words = lines.flatMap(_.split(" "))

    words.foreachRDD{(rdd:RDD[String],time:Time) =>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      wordsDataFrame.createOrReplaceTempView("words")

      val wordCountDataFrame =
        spark.sql("select word,count(*) as total from words group by word")
      println(s"================== $time =========================")
      wordCountDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }

  case class Record(word:String)

  object SparkSessionSingleton {
    @transient private var instance : SparkSession = _

    def getInstance(sparkConf: SparkConf):SparkSession = {
      if (instance == null) {
        instance = SparkSession
            .builder
            .config(sparkConf)
            .getOrCreate()
      }
      instance
    }
  }
}
