package com.cirrus.training

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.io.Source

object DataConsumer {
  def main(args: Array[String]): Unit = {
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    consumeFromKafka(props)
  }

  def consumeFromKafka(props: Properties) = {
    var topic = props.getProperty("topic.name", "tp_in_profileupdates");
    props.put("bootstrap.servers", props.getProperty("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667"));
    props.put("key.deserializer", props.getProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer"));
    props.put("value.deserializer", props.getProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));
    props.put("auto.offset.reset", props.getProperty("auto.offset.reset", "earliest"))
    props.put("group.id", props.getProperty("group.id", "GRP_" + topic))
    println("Starting Consumer with properties:" + props);

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    println("Consumer:" + consumer);

    consumer.subscribe(util.Arrays.asList(topic))
    println("Listening on topic:" + topic);

    while (true) {
      val record = consumer.poll(1000).asScala
      println("Consumer::Received record" + record)
      for (data <- record.iterator)
        println("Consumer::Data:\n" + data.value())
    }
    consumer.close()
    println("Consumer::Closed successfully")
  }
}
