package com.spark.ml.regression

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Logistic {
  def main(args:Array[String])= {
    val conf = new SparkConf().setAppName("linear").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val file = spark.read.format("csv").option("sep",";").option("header","true").load("C:\\Users\\hasee\\Desktop\\test.txt")
    //强制转换 shuffle将样本顺序打乱
    import spark.implicits._
    val random = new util.Random()
    val data = file.select("square","price").map(
      row => (row.getAs[String](0).toDouble,row.getString(1).toDouble,random.nextDouble())
    ).toDF("square","price","random").sort("random")
    //做训练参数的指定
    val assembler = new VectorAssembler().setInputCols(Array("square")).setOutputCol("features")
    val dataset = assembler.transform(data)
    var Array(train,test) = dataset.randomSplit(Array(0.8,0.2),1234L)

    //train.show()
    /**
      * fit 做训练
      * transform 做预测
      */
    val lr = new LogisticRegression().setLabelCol("price").setFeaturesCol("features").setRegParam(0.3).setElasticNetParam(0.8).setMaxIter(10)
    val model = lr.fit(train)
    model.transform(test).show()
    val s = model.summary.totalIterations
    println(s"iter:${s}")
  }
}
