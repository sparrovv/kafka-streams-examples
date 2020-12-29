package com.mwrobel.kafkastreams.example_copartioning

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties

/*
  Case 1. Topics are not partitioned the same way: Topics not co-partitioned: [contact_details_2p,quotes_created_3p]
     Exception in thread "my-first-application-0df76f55-0abd-4d8a-b5f6-cc6aa663a7a0-StreamThread-1"
     org.apache.kafka.streams.errors.TopologyException: Invalid topology:
     stream-thread [my-first-application-0df76f55-0abd-4d8a-b5f6-cc6aa663a7a0-StreamThread-1-consumer]
     Topics not co-partitioned: [contact_details_2p,quotes_created_3p]

  Case 2. They need to be co-partitioned with the same key.
  With Rekey method, kafka streams that does automagically.

  It must does something on the runtime?
    - it's on the runtime as it fetches metadata information about the topics and try to create the internal ones
    -

  How to test it out?
  - change the number of partitions on one topic, without changing anything else
    - from 3 to 5, and check what happens
    - remove and add back that topic
    When I've done it, kafka stream threw an exception:
      has invalid partitions: expected: 5; actual: 3. Use 'kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.
 */
object Main extends App with LazyLogging {
  val builder: StreamsBuilder = new StreamsBuilder

  TopologyWithJoins.buildTopology()(builder)

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
