package com.cirrus.training

import java.util.Properties
import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, HashMap, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{ArrayType, DateType, StringType, StructType}
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import com.databricks.spark.avro._
import org.apache.spark.streaming.dstream.DStream

import scala.io.Source


object FileSparkStream {

  /*
For reading data from files on any file system compatible with the HDFS API (that is, HDFS, S3, NFS, etc.)

DStream can be created as via StreamingContext.fileStream[KeyClass, ValueClass, InputFormatClass].

File streams do not require running a receiver so there is no need to allocate any cores for receiving file data.

Spark Streaming will monitor the directory dataDirectory and process any files created in that directory.
All files directly under such a path "hdfs://namenode:8040/logs/2020*" will be processed as they are discovered

Note:
To guarantee that changes are picked up in a window, write the file to an unmonitored directory, then,
immediately after the output stream is closed, rename it into the destination directory
Object Stores such as Amazon S3 and Azure Storage usually have slow rename operations, as the data is actually copied.
*/

  def main(args: Array[String]) {
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    val appName = props.getProperty("app.name", "Spark-File-Stream") //name of the application
    val interval = props.getProperty("micro.batch.interval", "5").toInt //micro batch interval
    println("main::appName:" + appName + ",interval:" + interval)
    val sparkConf = new SparkConf().setAppName(appName)
    println("main::sparkConf:" + sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    println("main::ssc:" + ssc)

    val dataDirectory = props.getProperty("file.stream.input.path", "/tmp/Spark-Kafka-Stream/Input-Files/")
    println("main::dataDirectory:" + dataDirectory)
    val textFileStream = ssc.textFileStream(dataDirectory)
    println("main::textFileStream:" + textFileStream)

    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    /*
    case class TextLine(line: String)
    val inputLinesDS: DStream[TextLine] = textFileStream.map(TextLine)
    println("main::inputLinesDS:" + inputLinesDS)
    inputLinesDS.foreachRDD(rdd => {
      rdd.foreach(println)
    })
    */

    val dynamicPartCols = "datepart"
    val sm = SaveMode.Append
    val outdir = props.getProperty("outputDirectory", "/tmp" + appName + "/Output-FS-")

    textFileStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(println)

        def dateParse = udf((rd1: String) => {
          val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'kk:mm:ss")
          println("rdd::Inside UDF:dataformat:" + DATE_TIME_FORMATTER + ", input:" + rd1)
          LocalDateTime.parse(rd1, DATE_TIME_FORMATTER).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        })

        val rddCache = rdd.repartition(1).cache()
        println("rdd::rddCache::" + rddCache)
        val rddDF = rddCache.toDF()
        println("rdd::rddDF::" + rddDF)
        rddDF.printSchema()
        rddDF.show(2)
        val enrichedUPDF = rddDF.withColumn(dynamicPartCols, dateParse($"date"))
        println("rdd::enrichedUPDF::" + enrichedUPDF)
        enrichedUPDF.printSchema()
        enrichedUPDF.show(2)
        enrichedUPDF.write.partitionBy(dynamicPartCols).format("orc").mode(sm).save(outdir + "ORC/")
      }
    })

    //Start the application and receive data
    ssc.start()
    //Await until termination
    ssc.awaitTermination()
  }
}
