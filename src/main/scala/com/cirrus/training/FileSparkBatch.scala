package com.cirrus.training

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.io.Source
import org.apache.spark.sql.functions._

object FileSparkBatch {

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
"intalli-j": edit and compile-package your code
"cloud-shell:" rm spark-streaming_2.11-0.1.jar (existing)
"cloud-shell:" upload the spark-streaming_2.11-0.1.jar
"cloud-shell": rm existing log-aggregation.csv
"cloud-shell": upload new log-aggregation.csv
"cloud-shell": scp log-aggregation.csv to instance-1 spark cluster
>>>gcloud beta compute scp --project="wired-effort-272101" --zone="us-east1-b" --recurse ./log-aggregations.csv     "instance-1":~/
"cloud-shell": scp spark-streaming_2.11-0.1.jar to instance-1 spark cluster
>>>gcloud beta compute scp --project="wired-effort-272101" --zone="us-east1-b" --recurse ./spark-streaming_2.11-0.1.jar     "instance-1":~/
"cloud-shell": ssh to intance-1
>>>gcloud beta compute ssh --zone "us-east1-b" "instance-1" --project "wired-effort-272101"
"instance-1": hadoop fs -put -f ~/log-aggregations.csv /tmp/Spark-Kafka-Stream/Input-Files/
"instance-1": start-file-batch.sh
"instance-1": tail -f log.spark.file.batch
*/

  def main(args: Array[String]) {
    println("------ START::FileSparkBatch ------")
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    val appName = props.getProperty("app.name", "Spark-File-Batch") //name of the application
    val dataDirectory = props.getProperty("file.stream.input.path", "/tmp/Spark-Kafka-Stream/Input-Files/")
    println("main::appName:" + appName + ",input.dataDirectory:" + dataDirectory)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    println("main::spark:" + spark)

    val sqlContext = new SQLContext(spark.sparkContext)
    println("main::sqlContext:" + sqlContext)

    import sqlContext.implicits._

    val csvDF = spark.read.format("csv").options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(dataDirectory)
    println("main::csvDF:" + csvDF)
    csvDF.printSchema()
    csvDF.show(2)

    csvDF.createOrReplaceTempView("app_logs")
    val moduleDF = spark.sql("SELECT * FROM app_logs where module='ModuleA'")
    println("main::moduleDF:" + moduleDF)
    moduleDF.printSchema()
    moduleDF.show(2)

    val dynamicPartCols = "datepart"
    var outdir = props.getProperty("outputDirectory", "/tmp" + appName + "/Output-FB-")
    val dateformat = props.getProperty("input.date.format", "yyyy-MM-dd kk:mm:ss")
    def dateParse = udf((rd1: String) => {
      val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(dateformat)
      println("main::Inside UDF:dataformat:" + DATE_TIME_FORMATTER + ", input:" + rd1)
      LocalDateTime.parse(rd1, DATE_TIME_FORMATTER).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    })
    val enrichedUPDF = csvDF.withColumn(dynamicPartCols, dateParse($"date"))
    println("main::enrichedUPDF::" + enrichedUPDF)
    enrichedUPDF.printSchema()
    enrichedUPDF.show(2)

    outdir = outdir + "-FB-ORC/"
    println("main::outdir:" + outdir)
    enrichedUPDF.write.partitionBy(dynamicPartCols).format("orc").mode(SaveMode.Append).save(outdir)

    println("------ THE END::FileSparkBatch ------")
  }
}
