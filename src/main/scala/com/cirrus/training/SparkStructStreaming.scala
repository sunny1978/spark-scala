package com.cirrus.training

import java.util.Properties

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import scala.io.Source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}


object SparkStructStreaming {

  def main(args: Array[String]) {
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    val appName = props.getProperty("app.name", "Spark-Kafka-Stream") //name of the application
    val brokers = props.getProperty("bootstrap.servers", "localhost:6667")

    val spark = SparkSession
      .builder
      .appName(appName)
      .master("yarn")
      .getOrCreate()
    println("main::spark:" + spark)
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", props.getProperty("topic.name"))
      .option("startingOffsets", "earliest")
      .load()
    println("main::df:" + df)

    val valDF = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("user_fname", StringType)
      .add("user_lname", StringType)
      .add("user_id", StringType)
      .add("updated_at", StringType)
      .add("modified_fields", ArrayType(StringType))

    val userProfileDF = valDF.select(from_json(col("value"), schema).as("data")).select("data.*")

    //Custom code goes here...

    val query1 = userProfileDF.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query1.awaitTermination()

  }

}
