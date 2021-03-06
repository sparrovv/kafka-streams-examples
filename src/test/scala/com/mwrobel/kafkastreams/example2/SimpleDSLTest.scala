package com.mwrobel.kafkastreams.example2

import java.util.Properties

import com.mwrobel.kafkastreams.Topics
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, Topology, TopologyTestDriver}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SimpleDSLTest extends FunSuite with BeforeAndAfter {
  var testDriver: TopologyTestDriver   = _
  implicit var builder: StreamsBuilder = _

  before {
    builder = new StreamsBuilder()
    MyAppTopology.buildTopology()
    val topology = builder.build()
    testDriver = setupTestDriver(topology)
  }

  after {
    testDriver.close()
  }

  def setupTestDriver(topology: Topology) = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    new TopologyTestDriver(topology, config)
  }

  val factory = new ConsumerRecordFactory(Topics.inputTopic, Serdes.String.serializer(), Serdes.String.serializer())

  test("testBuildTopology") {
    val consumerRecord = factory.create("some string that we will split by words and filter")
    testDriver.pipeInput(consumerRecord)

    val consumeFunc = () =>
      testDriver.readOutput(
        Topics.outputTopic,
        Serdes.String.deserializer(),
        Serdes.String.deserializer()
      )

    val output: List[String] = readUntilNoRecords(consumeFunc)

    assert(output == List("string", "filter"))
  }

  private def readUntilNoRecords[K, V](f: () => ProducerRecord[K, V], list: List[V] = List()): List[V] = {
    val record: ProducerRecord[K, V] = f()
    if (record == null) {
      list
    } else {
      readUntilNoRecords(f, list :+ record.value())
    }
  }

}
