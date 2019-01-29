package com.spark.ml.project1

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

object Project1 {
  def main(args:Array[String])= {
    val conf = new SparkConf().setAppName("iris").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //数据预处理
    import spark.implicits._
    val random = new util.Random()
    val neg = spark.read.textFile("neg.txt").map(
      line => {
        (line.split(" ").filter(!_.equals(" ")),0,random.nextDouble())
      }
    ).toDF("words","value","random")
    val pos = spark.read.textFile("pos.txt").map(
      line => {
        (line.split(" ").filter(!_.equals(" ")),1,random.nextDouble())
      }
    ).toDF("words","value","random")
    val data = neg.union(pos).sort("random")
    data.show(false)
    println(neg.count(),data.count())
    //文本特征值抽取
    val hashingTf = new HashingTF().setInputCol("words").setOutputCol("hashing").transform(data)
    val idfModel = new IDF().setInputCol("hashing").setOutputCol("tfidf").fit(hashingTf)
    val transformedData = idfModel.transform(hashingTf)
    var Array(train,test) = transformedData.randomSplit(Array(0.7,0.3))

    val bayes = new NaiveBayes()
      .setFeaturesCol("tfidf")  //x
      .setLabelCol("value")   //y
      .fit(train)
    val result = bayes.transform(test)
    result.show(false)
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("value").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(result)
    println(s"""accuracy is $accuracy""")
  }
}
