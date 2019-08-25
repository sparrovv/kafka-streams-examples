package com.mwrobel.kafkastreams.example6

import java.time.Duration
import java.util.Properties

import com.mwrobel.kafkastreams.example1.Main.{builder, logger}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

object Main extends App with LazyLogging {
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  implicit val builder: StreamsBuilder = new StreamsBuilder

  TopologyWithSchedule.buildTopology()

  val topology: Topology = builder.build()
  logger.info(topology.describe().toString)

  val streams: KafkaStreams = new KafkaStreams(topology, props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
