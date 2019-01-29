package com.spark.ml.classification

import org.apache.spark.SparkConf
//import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
  * 要求spark2.3.0版本
  */
object Svm {
  def main(args:Array[String])= {
    val conf = new SparkConf().setAppName("iris").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val file = spark.read.format("csv").option("sep"," ").option("header","true").load("iris.txt")
    //file.show()
    //强制转换 shuffle将样本顺序打乱
    import spark.implicits._
    val random = new util.Random()
    val data = file.map(
      row => {
        val label = row.getString(5) match {
          case "setosa" => 0
          case "versicolor" => 1
          case "virginica" => 2
        }
        (row.getString(0).toDouble,
          row.getString(1).toDouble,
          row.getString(2).toDouble,
          row.getString(3).toDouble,
          row.getString(4).toDouble,
          label, random.nextDouble())
      }).toDF("num","c0","c1","c2","c3","label","random").sort("random").where("label=1 or label=0")
    //做训练参数的指定
    val assembler = new VectorAssembler().setInputCols(Array("c0","c1","c2","c3")).setOutputCol("features")
    val dataset = assembler.transform(data)
    var Array(train,test) = dataset.randomSplit(Array(0.8,0.2))

    //svm
  /*  val svm = new LinearSVC().setMaxIter(20).setRegParam(0.1).setFeaturesCol("features").setLabelCol("label")
    val model = svm.fit(train)
    model.transform(test).show()*/
  }
}
