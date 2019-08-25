package com.mwrobel.kafkastreams.example6

import java.util.Properties

import com.mwrobel.kafkastreams.LeadManagementTopics
import com.mwrobel.kafkastreams.example6.models._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, Topology, TopologyTestDriver}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SchedulerTest extends FunSuite with BeforeAndAfter {

  var testDriver: TopologyTestDriver   = _
  implicit var builder: StreamsBuilder = _

  before {
    builder = new StreamsBuilder()
    TopologyWithSchedule.buildTopology()
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
      LeadManagementTopics.contactDetailsEntity,
      Serdes.String.serializer(),
      ContactDetailsEntity.serde.serializer()
    )
  val quotesCreatedFactory =
    new ConsumerRecordFactory(
      LeadManagementTopics.quotesCreated,
      Serdes.String.serializer(),
      QuotesCreated.serde.serializer()
    )

  val contactDetails = ContactDetailsEntity(id = "1", name = "Michal", telephoneNumber = "1")
  val quotesCreatedEvent1 = QuotesCreated(
    eventId = "xx1",
    reference = "ref1",
    userId = contactDetails.id,
    quotesNumber = 1
  )
  val quotesCreatedEvent2 = QuotesCreated(
    eventId = "xx2",
    reference = "ref2",
    userId = contactDetails.id,
    quotesNumber = 3
  )

  test("testBuildTopology") {
    val consumerRecord =
      contactDetailsFactory.create(LeadManagementTopics.contactDetailsEntity, contactDetails.id, contactDetails)
    val quotesCreatedRecord = quotesCreatedFactory.create(
      LeadManagementTopics.quotesCreated,
      quotesCreatedEvent1.eventId,
      quotesCreatedEvent1
    )
    val quotesCreatedRecord2 = quotesCreatedFactory.create(
      LeadManagementTopics.quotesCreated,
      quotesCreatedEvent2.eventId,
      quotesCreatedEvent2
    )

    testDriver.pipeInput(consumerRecord)
    testDriver.pipeInput(quotesCreatedRecord)
    testDriver.pipeInput(quotesCreatedRecord2)

    val consumeFunc = () =>
      testDriver.readOutput(
        LeadManagementTopics.contactRequests,
        Serdes.String.deserializer(),
        ContactRequest.serde.deserializer()
      )

    assert(readUntilNoRecords(consumeFunc).size == 0)

    testDriver.advanceWallClockTime(11000)
    assert(readUntilNoRecords(consumeFunc).size == 0)

    testDriver.advanceWallClockTime(120000)
    assert(readUntilNoRecords(consumeFunc).size == 1)
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
