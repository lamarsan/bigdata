//package com.spark.ml.project2
//
//import org.apache.spark.SparkConf
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.recommendation._
//import org.apache.spark.ml.recommendation.ALS.Rating
//import org.apache.spark.sql.SparkSession
//
//object Als {
//  def main(args:Array[String])= {
//    val conf = new SparkConf().setAppName("RS").setMaster("local[2]")
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    spark.sparkContext.setLogLevel("WARN")
//
//    val parseRating = (string: String) => {
//      val stringArray = string.split("\t")
//      Rating(stringArray(0).toInt,stringArray(1).toInt,stringArray(2).toFloat)
//    }
//
//    import spark.implicits._
//    val data = spark.read.textFile("u.txt")
//      .map(parseRating)
//      .toDF("userID","itemID","rating")
//    var Array(train,test) = data.randomSplit(Array(0.8,0.2))
//
//    //用来矩阵分解
//    val als = new ALS()
//      .setMaxIter(20)
//      .setUserCol("userID")
//      .setItemCol("itemID")
//      .setRatingCol("rating")
//      .setRegParam(0.01) //正则化参数
//
//    val model = als.fit(train)
//    model.setColdStartStrategy("drop")  //冷启动策略，这是推荐系统的一个重点内容
//
//    val predictions = model.transform(test)
//    //predictions.show(false)
//
//    val users = spark.createDataset(Array(196)).toDF("userID")
//    //users.show(false)
//    model.recommendForUserSubset(users,10).show(false)
//
//    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("rating").setPredictionCol("prediction").setMetricName("rmse")
//    val rmse = evaluator.evaluate(predictions)
//    println(s"Root-mean-square error is $rmse \n")
//  }
//}
