package com.scala.krishna


import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object RegressionMl {
 
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Krishna-Ml-Testing")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val input = hiveContext.sql(s""" Select * from ****.cancer """)
    input.printSchema()
    val selected = input.select(when(col("class").equalTo(lit(4.0)),lit(1)).otherwise(lit(0)).as("clas"),
        col("clump"), col("uniformity_of_cell_size"), col("uniformity_of_cell_shape"), 
        col("marginal_adhesion"), col("single_epithelial_cell_size"), col("bare_nuclei"), col("bland_chromatin"), col("normal_nucleoli"), col("mitoses"))
    println("Only Selected Cols")
    selected.show()
    selected.printSchema()
    val featureCols = Array("clump", "uniformity_of_cell_size", "uniformity_of_cell_shape", "marginal_adhesion", "single_epithelial_cell_size", "bare_nuclei", "bland_chromatin", "normal_nucleoli", "mitoses")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(selected)
    println("Transform:")
    df2.show()
   
    val labelIndexer = new StringIndexer().setInputCol("clas").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
     val splitSeed = 5043
     println("Class Variable Declaration Done:")
    df3.show()
    
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    println("Printing Training Data")
    trainingData.rdd.collect()
    println("Printing Testing Data")
    testData.rdd.collect()
    
    
    val model = lr.fit(trainingData)
    val predictions = model.transform(testData)
    println("Model Building DOne:")
    predictions.show()
   
    
    
  }
}