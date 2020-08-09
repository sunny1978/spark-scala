package com.cirrus.training

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.io.Source
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

object FileSparkBatchHBase {

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

  object MyAverage extends UserDefinedAggregateFunction {
    // Data types of input arguments of this aggregate function
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
    // Data types of values in the aggregation buffer
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }
    // The data type of the returned value
    def dataType: DataType = DoubleType
    // Whether this function always returns the same output on the identical input
    def deterministic: Boolean = true
    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      println("MyAverage::initialize:buffer:" + buffer)
      buffer(0) = 0L
      buffer(1) = 0L
    }
    // Updates the given aggregation buffer `buffer` with new input data from `input`
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println("MyAverage::update:buffer:" + buffer + ",input:" + input)
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }
    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      println("MyAverage::merge:buffer1:" + buffer1 + ",buffer2:" + buffer2)
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    // Calculates the final result
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  def main(args: Array[String]) {
    println("------ START::FileSparkBatch ------")
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    val appName = props.getProperty("app.name", "Spark-File-Batch-HBase") //name of the application
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

    //Custom UDF
    // Register the function to access it
    spark.udf.register("myAverage", MyAverage)
    //select ColA, concat(Colc) from table group by ColA
    val result = spark.sql("SELECT myAverage(response_time) as avg_response_time FROM app_logs")
    result.printSchema()
    result.show(2)

    //Inline UDF - Enrich
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

    //Write to hbase table 'log_agg'
    //rowkey aka primary key - is a string
    //a. Define a catalog schema for the HBase table named Contacts.
    //b. Identify the rowkey as key, and map the column names used in Spark to the column family, column name, and column type as used in HBase.
    //c. The rowkey also has to be defined in detail as a named column (rowkey), which has a specific column family cf of rowkey.
    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"log_agg"},
                     |"rowkey":"key",
                     |"columns":{
                     |"key":{"cf":"rowkey", "col":"key", "type":"long"},
                     |"application":{"cf":"cf", "col":"application", "type":"string"},
                     |"module":{"cf":"cf", "col":"module", "type":"string"},
                     |"message":{"cf":"cf", "col":"message", "type":"string"}
                     |}
                     |}""".stripMargin
    println("main::hbase:catalog:" + catalog)
    case class LogAggRecord(rowkey: String,application: String,module: String, message: String)
    var hbaseDF = enrichedUPDF.select("application","module", "message")
    hbaseDF = hbaseDF.withColumn("key", monotonicallyIncreasingId) //we need a unique key, since log_agg dont have, I am adding one.
    hbaseDF.printSchema()
    hbaseDF.show(2)

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/usr/hdp/current/hbase-client/conf/hbase-site.xml"))
    new HBaseContext(spark.sparkContext, conf)
    hbaseDF.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.hadoop.hbase.spark").save()

    println("------ THE END::FileUDFSparkBatch ------")
  }
}

