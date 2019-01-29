package com.spark.ml.classification

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Dt {
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
      }).toDF("num","c0","c1","c2","c3","label","random").sort("random")
//    //做训练参数的指定
    val assembler = new VectorAssembler().setInputCols(Array("c0","c1","c2","c3")).setOutputCol("features")
    val dataset = assembler.transform(data)
    var Array(train,test) = dataset.randomSplit(Array(0.8,0.2))

    val dt = new DecisionTreeClassifier().setFeaturesCol("features").setLabelCol("label")
    val model = dt.fit(train)
    val result = model.transform(test)
    result.show()
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(result)
    println(s"""accuracy is $accuracy""")
  }
}
