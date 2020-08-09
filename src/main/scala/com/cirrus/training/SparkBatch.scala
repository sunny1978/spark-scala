package com.cirrus.training
import org.apache.spark.{SparkConf, SparkContext}

object SparkBatch {
  val appName = "Spark-Training"
  val master = "master"
  val conf = new SparkConf().setMaster(master).setAppName(appName)
  val sc = new SparkContext(conf)
  println("SparkMain::SparkContext:" + sc.version)
}
