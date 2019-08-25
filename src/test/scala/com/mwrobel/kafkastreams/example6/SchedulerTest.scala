package com.mwrobel.kafkastreams.example6

import java.util.Properties

import com.mwrobel.kafkastreams.example6.models._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.KeyValueStore
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

  val customerFactory =
    new ConsumerRecordFactory(Topics.customerTopic, Serdes.String.serializer(), Customer.serde.serializer())
  val rfqCreatedFactory =
    new ConsumerRecordFactory(Topics.rfqCreateTopic, Serdes.String.serializer(), RfqCreatedEvent.serde.serializer())

  val customer = Customer(id = "1", name = "Michal", telephoneNumber = "1")
  val rfqCreatedEvent1 = RfqCreatedEvent(
    eventId = "xx1",
    rfqReference = "ref1",
    customerId = customer.id,
    quotesNumber = 1,
    decision = Decision.Quoted
  )
  val rfqCreatedEvent2 = RfqCreatedEvent(
    eventId = "xx2",
    rfqReference = "ref2",
    customerId = customer.id,
    quotesNumber = 3,
    decision = Decision.Referred
  )

  test("testBuildTopology") {
    val consumerRecord = customerFactory.create(Topics.customerTopic, customer.id, customer)
    val rfqCreatedRecord = rfqCreatedFactory.create(
      Topics.rfqCreateTopic,
      rfqCreatedEvent1.eventId,
      rfqCreatedEvent1
    )
    val rfqCreatedRecord2 = rfqCreatedFactory.create(
      Topics.rfqCreateTopic,
      rfqCreatedEvent2.eventId,
      rfqCreatedEvent2
    )

    testDriver.pipeInput(consumerRecord)
    testDriver.pipeInput(rfqCreatedRecord)
    testDriver.pipeInput(rfqCreatedRecord2)

    val consumeFunc = () =>
      testDriver.readOutput(
        Topics.contactRequests,
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
