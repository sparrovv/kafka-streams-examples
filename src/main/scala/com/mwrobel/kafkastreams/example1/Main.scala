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

  // streams builder - provides high-level DSL
  val builder: StreamsBuilder = new StreamsBuilder

  // actual logic of the application
  val source: KStream[String, String] = builder
    .stream[String, String](Topics.inputTopic)

  source
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
    .filter((_, word) => word.length() > 5)
    .to(Topics.outputTopic)

  // creating topology
  val topology: Topology = builder.build()
  logger.info(topology.describe().toString)

  // running kafka streams
  val streams: KafkaStreams = new KafkaStreams(topology, properties)
  streams.start()

  // graceful shutdown, so there's time to gracefully shutdown kafka streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

  // Settings
  def properties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

}
