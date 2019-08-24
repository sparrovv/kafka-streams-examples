package com.mwrobel.kafkastreams.example1

import java.time.Duration
import java.util.Properties

import com.mwrobel.kafkastreams.Topics
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

object Main extends App with LazyLogging {
  import Serdes._

  val builder: StreamsBuilder = new StreamsBuilder

  val source: KStream[String, String] = builder
    .stream[String, String](Topics.inputTopic)

  source
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
    .filter((_, word) => word.length() > 5)
    .to(Topics.outputTopic)

  val topology: Topology = builder.build()
  logger.info(topology.describe().toString)

  val streams: KafkaStreams = new KafkaStreams(topology, kafkaStreamsProps)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

  def kafkaStreamsProps: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }
}
