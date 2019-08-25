package com.mwrobel.kafkastreams.example5

import java.util.Properties

import com.mwrobel.kafkastreams.example5.models._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, Topology, TopologyTestDriver}
import org.scalatest.{BeforeAndAfter, FunSuite}

class DeduplicationTest extends FunSuite with BeforeAndAfter {

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
      Topics.contactDetails,
      Serdes.String.serializer(),
      ContactDetailsEntity.serde.serializer()
    )
  val quotesCreattedFactory =
    new ConsumerRecordFactory(Topics.quotesCreated, Serdes.String.serializer(), QuotesCreated.serde.serializer())

  val contactDetails = ContactDetailsEntity(id = "1", name = "Michal", telephoneNumber = "1")
  val quotesCreattedEvent1 = QuotesCreated(
    eventId = "xx1",
    reference = "ref1",
    userId = contactDetails.id,
    quotesNumber = 1
  )
  val quotesCreattedEvent2 = QuotesCreated(
    eventId = "xx2",
    reference = "ref2",
    userId = contactDetails.id,
    quotesNumber = 3
  )

  test("testBuildTopology") {
    val consumerRecord = contactDetailsFactory.create(Topics.contactDetails, contactDetails.id, contactDetails)
    val quotesCreattedRecord = quotesCreattedFactory.create(
      Topics.quotesCreated,
      quotesCreattedEvent1.eventId,
      quotesCreattedEvent1
    )
    val quotesCreattedRecord2 = quotesCreattedFactory.create(
      Topics.quotesCreated,
      quotesCreattedEvent2.eventId,
      quotesCreattedEvent2
    )

    testDriver.pipeInput(consumerRecord)
    testDriver.pipeInput(quotesCreattedRecord)
    testDriver.pipeInput(quotesCreattedRecord2)

    val consumeFunc = () =>
      testDriver.readOutput(
        Topics.contactRequests,
        Serdes.String.deserializer(),
        ContactRequest.serde.deserializer()
      )
    val result: Seq[ContactRequest] = readUntilNoRecords(consumeFunc)

    assert(result.size == 1)

    val store: KeyValueStore[String, ContactRequest] = testDriver.getKeyValueStore(ContactRequestsStore.name)
    val contactRequestInStore                        = Option(store.get(contactDetails.id))

    assert(result(0).copy(deduplicatedNumber = 1) == contactRequestInStore.get)
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
