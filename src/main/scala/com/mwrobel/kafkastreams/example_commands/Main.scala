package com.mwrobel.kafkastreams.example_commands

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties

object Main extends App with LazyLogging {
  val builder: StreamsBuilder = new StreamsBuilder

  CommandsTopology.buildTopology()(builder)

  val topology: Topology = builder.build()
  logger.info(topology.describe().toString)

  val streams: KafkaStreams = new KafkaStreams(topology, kafkaStreamsProps)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

  def kafkaStreamsProps: Properties = {
    val p = new Properties()

    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    p
  }
}
