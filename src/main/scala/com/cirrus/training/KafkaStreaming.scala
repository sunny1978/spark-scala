package com.cirrus.training

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KeyValueMapper}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder}

import scala.io.Source

/**
 * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic SourceTopic
 * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic SinkTopic
 *
 * Produce Data:
 * /usr/hdp/3.0.1.0-187/kafka/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667 --topic SourceTopic
 */
object KafkaStreaming {

  def main(args: Array[String]) {

    var propFile = args(0);
    println("main::Reading property file:" + propFile);

    val source = Source.fromFile(propFile)
    val br = source.bufferedReader()
    var props = new Properties();
    props.load(br);
    println("main::Properties:" + props)

    startApp(props)
  }

  def startApp(props: Properties) {
    //props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getProperty("application.id", "Kafka-Streams-Training"))
    //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667"))
    //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    println("startApp::props:" + props)

    val builder: StreamsBuilder = new StreamsBuilder()
    println("startApp::builder:" + builder)
    val textLines: KStream[String, String] = builder.stream[String, String](props.getProperty("source.topic","SourceTopic"))
    println("startApp::textLines:" + textLines)
    val kvs = textLines.map[String, String]{
      new KeyValueMapper[String, String, KeyValue[String, String]] {
        override def apply(key: String, value: String): KeyValue[String, String] = {
          //perform some transformation on value.
          println("startApp::map.apply:" + value)
          new KeyValue(key, value)
        }
      }
    }

    val stringSerde = Serdes.String()
    kvs.to(stringSerde, stringSerde, props.getProperty("sink.topic","SinkTopic"))

    val streams = new KafkaStreams(builder.build(), props)
    println("startApp::Starting Streaming Application")
    streams.start()
    println("startApp::Started Streaming Application")
  }
}
