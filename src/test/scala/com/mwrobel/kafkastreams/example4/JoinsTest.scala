package com.mwrobel.kafkastreams.example4

import java.util.Properties

import com.mwrobel.kafkastreams.example4.models._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, Topology, TopologyTestDriver}
import org.scalatest.{BeforeAndAfter, FunSuite}

class JoinsTest extends FunSuite with BeforeAndAfter {
  var testDriver: TopologyTestDriver   = _
  implicit var builder: StreamsBuilder = _

  before {
    builder = new StreamsBuilder()
    TopologyWithStateStore.buildTopology()
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

  val contactDetailsFactory =
    new ConsumerRecordFactory(
      Topics.contactDetailsEntity,
      Serdes.String.serializer(),
      ContactDetailsEntity.serde.serializer()
    )

  val rfqCreatedFactory =
    new ConsumerRecordFactory(Topics.quotesCreated, Serdes.String.serializer(), QuotesCreated.serde.serializer())

  test("testBuildTopology") {
    val contactDetailsEntity = ContactDetailsEntity(id = "1", name = "Michal", telephoneNumber = "1")
    val quotesCreated = QuotesCreated(
      eventId = "xx",
      reference = "ref1",
      userId = contactDetailsEntity.id,
      quotesNumber = 1,
    )

    val consumerRecord = contactDetailsFactory.create(Topics.contactDetailsEntity, contactDetailsEntity.id, contactDetailsEntity)
    val rfqCreatedRecord = rfqCreatedFactory.create(
      Topics.quotesCreated,
      quotesCreated.eventId,
      quotesCreated
    )

    testDriver.pipeInput(consumerRecord)
    testDriver.pipeInput(rfqCreatedRecord)

    val consumeFunc = () =>
      testDriver.readOutput(
        Topics.contactRequests,
        Serdes.String.deserializer(),
        ContactRequest.serde.deserializer()
      )
    val result: Seq[ContactRequest] = readUntilNoRecords(consumeFunc)

    assert(result.size == 1)
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
