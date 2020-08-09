package com.cirrus.training
import java.util.Properties

import org.apache.kafka.clients.producer._

import scala.io.Source

/**
 * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic tp_in_profileupdates
 */
object DataProducer {
  def main(args: Array[String]): Unit = {
    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)
    writeToKafka(props);
  }

  def writeToKafka(props: Properties): Unit = {
    var topic = props.getProperty("topic.name", "tp_in_profileupdates");
    props.put("bootstrap.servers", props.getProperty("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667"));
    props.put("key.serializer", props.getProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer"));
    props.put("value.serializer", props.getProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
    println("DataProducer::Starting Producer with properties:" + props);
    var producer = new KafkaProducer[String, String](props);
    println("KafkaProducer:" + producer);
    var topicKey = props.getProperty("topic.key");
    var rec = "{\"user_fname\":\"sunil\", \"user_lname\":\"miriyala\", \"user_id\":\"sunil.miriyala\", \"updated_at\":\"2020-03-22 14:15:16.000\", \"modified_fields\":[{\"address\":\"new value\"}]} "
    var message = props.getProperty("message", rec);
    var record = new ProducerRecord[String, String](topic, topicKey, message)
    println("DataProducer::ProduceRecord:" + record)
    producer.send(record)
    println("DataProducer::Produced successfully")
    producer.close()
    println("DataProducer::Closed successfully")
  }

}
