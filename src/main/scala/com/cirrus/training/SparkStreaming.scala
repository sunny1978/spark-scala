package com.cirrus.training

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.{col, from_json, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.io.Source


case class KafkaPayload(value: String)

/**
 * /usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh --bootstrap-server instance-1.us-east1-b.c.wired-effort-272101.internal:6667 --describe --group SparkStreaming
 */

object SparkStreaming {

  def main(args: Array[String]) {
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    val appName = props.getProperty("app.name", "Spark-Kafka-Stream") //name of the application
    val interval = props.getProperty("micro.batch.interval", "5").toInt //micro batch interval

    val sparkConf = new SparkConf().setAppName(appName)
    println("sparkConf:" + sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    defineApp(props, ssc)

    //Start the application and receive data
    ssc.start()
    //Await until termination
    ssc.awaitTermination()
  }

  def loadApp(props: Properties): StreamingContext = {
    val appName = props.getProperty("app.name", "Spark-Kafka-Stream") //name of the application
    val interval = props.getProperty("micro.batch.interval", "5").toInt //micro batch interval
    //setMaster(master) will manage using spark-submit.sh command
    val sparkConf = new SparkConf().setAppName(appName)
    println("sparkConf:" + sparkConf)

    //main entry point of all Spark Streaming
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    println("StreamingContext:" + ssc)
    //internally creates SparkContext, ssc.sparkContext

    //Checkpoint
    val checkpointDirectory = props.getProperty("checkpointDirectory")
    if (checkpointDirectory != null) {
      ssc.checkpoint(checkpointDirectory)
    }
    defineApp(props, ssc)

    ssc
  }

  def defineApp(props: Properties, ssc: StreamingContext) {
    val sparkSession = SparkSession.builder().config(ssc.sparkContext.getConf)
    val spark = sparkSession.getOrCreate()
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._


    //Define params:
    val appName = props.getProperty("app.name", "Spark-Kafka-Stream") //name of the application
    val interval = props.getProperty("micro.batch.interval", "5").toInt //micro batch interval
    val brokers = props.getProperty("bootstrap.servers", "localhost:6667")
    val kd = props.getProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val vd = props.getProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val grp = props.getProperty("group.id", appName)
    val aor = props.getProperty("auto.offset.reset", "earliest")
    val sp = props.getProperty("security.protocol", "PLAINTEXT")
    val aoc = props.getProperty("enable.auto.commit", "true")
    val clientId = props.getProperty("client.id", appName)
    val bs = props.getProperty("bootstrap.servers", "localhost:6667")
    val outdir = props.getProperty("outputDirectory", "/tmp" + appName + "/Output")

    //, "bootstrap.servers" -> bs, "client.id" -> clientId, "key.deserializer" -> kd, "value.deserializer" -> vd, "group.id" -> grp, "auto.offset.reset" -> aor, "security.protocol" -> sp, "enable.auto.commit" -> aoc
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "bootstrap.servers" -> bs, "client.id" -> clientId, "key.deserializer" -> kd, "value.deserializer" -> vd, "group.id" -> grp, "auto.offset.reset" -> aor, "security.protocol" -> sp, "enable.auto.commit" -> aoc)
    println("kafkaParams:" + kafkaParams)
    val topicsSet = props.getProperty("topic.name").split(",").toSet
    println("topicsSet:" + topicsSet)

    //What is your input source? Define here.
    //Key: A String
    //Value: A String
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    //message:org.apache.spark.streaming.kafka010.DirectKafkaInputDStream
    println("dstream:" + dstream)

    val schema = new StructType()
      .add("user_fname", StringType)
      .add("user_lname", StringType)
      .add("user_id", StringType)
      .add("updated_at", StringType)
      .add("modified_fields", ArrayType(StringType))

    val ymd = new SimpleDateFormat("yyyy-MM-dd")
    val dynamicPartCols = "datepart"
    val sm = SaveMode.Append

    //https://spark.apache.org/docs/2.3.1/streaming-kafka-0-10-integration.html
    //exactly-once
    //offsets will be fetched ONLY on the first MAP function call

    //Write to HDFS
    dstream.map(_.value()).foreachRDD(rdd => {
      //only applicable for first map call. if not expect CCE like:
      //java.lang.ClassCastException: org.apache.spark.rdd.MapPartitionsRDD cannot be cast to org.apache.spark.streaming.kafka010.HasOffsetRanges
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //println("offsetRanges:" + offsetRanges)

      println("LOOP2::rdd:" + rdd)
      //{"user_fname":"sunil", "user_lname":"miri", "user_id":"sunil.miri", "updated_at":"2020-03-30 14:15:16.000", "modified_fields":[{"address":"new value"}]}
      rdd.foreach(println)

      if (!rdd.isEmpty()) {

        saveAsTEXT(outdir, rdd)

        val rddDF = rdd.toDF()

        val userProfileDF: DataFrame = getAsUserProfileDF(schema, rddDF)

        val enrichedUDF: DataFrame = addDatePartition(dynamicPartCols, userProfileDF)

        aggregateResponseTimes(spark, enrichedUDF)

        saveAsJSON(outdir, dynamicPartCols, sm, enrichedUDF)

        saveAsORC(outdir, dynamicPartCols, sm, enrichedUDF)

        saveAsAVRO(outdir, dynamicPartCols, sm, enrichedUDF)

        saveToHBASE(spark, enrichedUDF)
      }
      //dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

  }

  private def getAsUserProfileDF(schema: StructType, rddDF: DataFrame) = {
    println("getAsUserProfileDF::rddDF:" + rddDF)
    //datasets/dataframe
    rddDF.printSchema()
    rddDF.show(2)
    val valDF = rddDF.selectExpr("CAST(value AS STRING)")
    println("getAsUserProfileDF::valDF:" + valDF)
    val userProfileDF = valDF.select(from_json(col("value"), schema).as("data")).select("data.*")
    println("getAsUserProfileDF::userProfileDF:" + userProfileDF)
    userProfileDF.printSchema()
    userProfileDF.show(2)
    userProfileDF
  }

  private def saveAsAVRO(outdir: String, dynamicPartCols: String, sm: SaveMode, enrichedUDF: DataFrame) = {
    //saveAsAVRO::enrichedUDF:[user_fname: string, user_lname: string ... 4 more fields]
    println("saveAsAVRO::enrichedUDF:" + enrichedUDF)
    //HDFS:Write as AVRO
    val name = "MyAVRO"
    val namespace = "com.cirrus"
    val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)
    enrichedUDF.write.options(parameters).partitionBy(dynamicPartCols).format("com.databricks.spark.avro").mode(sm).save(outdir + "-AVRO/")
  }

  private def aggregateResponseTimes(spark: SparkSession, enrichedUDF: DataFrame) = {
    println("aggregateResponseTimes::enrichedUDF:" + enrichedUDF)
    //Spark-SQL:
    //gives some random value for each row
    spark.udf.register("myAverage", MyAverage)
    val enrichedUDF2 = enrichedUDF.withColumn("response_time", rand)
    println("aggregateResponseTimes::enrichedUDF2:" + enrichedUDF2)
    enrichedUDF2.createOrReplaceTempView("profile_updates")
    //select ColA, concat(Colc) from table group by ColA
    val myAvgUDF = spark.sql("SELECT myAverage(response_time) as avg_response_time FROM profile_updates")
    println("aggregateResponseTimes::myAvgUDF:" + myAvgUDF)
    myAvgUDF.printSchema()
    myAvgUDF.show(2)

    /**
     * root
     * |-- avg_response_time: double (nullable = true)
     * +-----------------+
     * |avg_response_time|
     * +-----------------+
     * |              0.0|
     * +-----------------+
     */
  }

  private def saveAsORC(outdir: String, dynamicPartCols: String, sm: SaveMode, enrichedUDF: DataFrame) = {
    //saveAsORC::enrichedUDF:[user_fname: string, user_lname: string ... 4 more fields]
    println("saveAsORC::enrichedUDF:" + enrichedUDF)
    //HDFS:Write as ORC
    enrichedUDF.write.partitionBy(dynamicPartCols).format("orc").mode(sm).save(outdir + "-ORC/")
  }

  private def saveAsJSON(outdir: String, dynamicPartCols: String, sm: SaveMode, enrichedUDF: DataFrame) = {
    //saveAsJSON::enrichedUDF:[user_fname: string, user_lname: string ... 4 more fields]
    println("saveAsJSON::enrichedUDF:" + enrichedUDF)
    //HDFS:Basic HDFS, with json format
    ///tmp/Spark-Kafka-Stream/Output-JSON
    enrichedUDF.write.partitionBy(dynamicPartCols).format("json").mode(sm).save(outdir + "-JSON/")
  }

  private def addDatePartition(dynamicPartCols: String, userProfileDF: DataFrame) = {
    println("addDatePartition::userProfileDF:" + userProfileDF)
    //Will parse input "updated_at" column and add "datepart" new column to DF
    //Helps parse input date column fields and transform.
    val enrichedUDF = userProfileDF.withColumn(dynamicPartCols, dateParse(col("updated_at")))
    println("addDatePartition::enrichedUDF:" + enrichedUDF)
    enrichedUDF.printSchema()
    enrichedUDF.show(2)

    /**
     * root
     * |-- modified_fields: array (nullable = true)
     * |    |-- element: struct (containsNull = true)
     * |    |    |-- address: string (nullable = true)
     * |-- updated_at: string (nullable = true)
     * |-- user_fname: string (nullable = true)
     * |-- user_id: string (nullable = true)
     * |-- user_lname: string (nullable = true)
     * |-- datepart: string (nullable = true)
     * +---------------+--------------------+----------+-----------+----------+----------+
     * |modified_fields|          updated_at|user_fname|    user_id|user_lname|  datepart|
     * +---------------+--------------------+----------+-----------+----------+----------+
     * |  [[new value]]|2020-03-30 14:15:...|     sunil|sunil.miri3|      miri|2020-03-30|
     * +---------------+--------------------+----------+-----------+----------+----------+
     */

    enrichedUDF
  }

  private def dateParse = udf((rd1: String) => {
    //dateParse::rd1:2020-03-30 14:15:16.000
    println("dateParse::rd1:" + rd1)
    val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss.SSS")
    println("Inside UDF:dataformat:" + DATE_TIME_FORMATTER + ", input:" + rd1)
    LocalDateTime.parse(rd1, DATE_TIME_FORMATTER).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  })

  private def saveAsTEXT(outdir: String, rdd: RDD[String]) = {
    //saveAsTEXT::rdd:MapPartitionsRDD[26] at map at SparkStreaming.scala:148
    println("saveAsTEXT::rdd:" + rdd)
    //HDFS:Basic HDFS.
    rdd.coalesce(1).saveAsTextFile(outdir)
  }

  private def saveToHBASE(spark: SparkSession, enrichedUDF: DataFrame): Unit = {
    println("saveToHBASE::enrichedUDF:" + enrichedUDF)

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"user_id",
         |"columns":{
         |"user_id":{"cf":"rowkey", "col":"user_id", "type":"string"},
         |"user_fname":{"cf":"cf", "col":"user_fname", "type":"string"},
         |"user_lname":{"cf":"cf", "col":"user_lname", "type":"string"}
         |}
         |}""".stripMargin

    println("saveToHBASE::catalog:" + catalog)

    var hbaseDF = enrichedUDF.select("user_id", "user_fname", "user_lname")
    println("saveToHBASE::hbaseDF:" + hbaseDF)
    hbaseDF.printSchema()
    hbaseDF.show(2)

    /**
     * saveToHBASE::enrichedUDF:[modified_fields: array<struct<address:string>>, updated_at: string ... 4 more fields]
     * saveToHBASE::catalog:{
     * "table":{"namespace":"default", "name":"user_profile"},
     * "rowkey":"user_id",
     * "columns":{
     * "user_id":{"cf":"rowkey", "col":"user_id", "type":"string"},
     * "user_fname":{"cf":"cf", "col":"user_fname", "type":"string"},
     * "user_lname":{"cf":"cf", "col":"user_lname", "type":"string"}
     * }
     * }
     * saveToHBASE::hbaseDF:[user_id: string, user_fname: string ... 1 more field]
     * root
     * |-- user_id: string (nullable = true)
     * |-- user_fname: string (nullable = true)
     * |-- user_lname: string (nullable = true)
     * +-----------+----------+----------+
     * |    user_id|user_fname|user_lname|
     * +-----------+----------+----------+
     * |sunil.miri3|     sunil|      miri|
     * +-----------+----------+----------+
     */

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/usr/hdp/current/hbase-client/conf/hbase-site.xml"))
    new HBaseContext(spark.sparkContext, conf)
    hbaseDF.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.hadoop.hbase.spark").save()
  }

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

}
