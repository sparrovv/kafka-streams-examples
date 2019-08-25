package com.mwrobel.kafkastreams.example6

import java.time

import com.mwrobel.kafkastreams.example6.models._
import com.mwrobel.kafkastreams.example6.utils.CloseableResource
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier, ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.joda.time.{DateTime, DateTimeZone}

object StreamEnricher {
  implicit def toStreamEnricher[K, V](ks: KStream[K, V]) = new StreamEnricher(ks)

  class StreamEnricher[K, V](kstream: KStream[K, V]) {
    def notNull: KStream[K, V] = {
      kstream.filter((_, v) => v != null)
    }
  }
}
object Topics {
  val customerTopic   = "customers"
  val contactRequests = "contact_requests"
  val rfqCreateTopic  = "rfq_created"
}

object ContactRequestsStore {
  val name = "contact_requests_store"

  val keySerde = Serdes.String
  val valSerde = ContactRequest.serde

  type MySuperStore = KeyValueStore[String, ContactRequest]
}

class StoreAndDeduplicateContactRequests extends ValueTransformer[ContactRequest, ContactRequest] {
  var myStateStore: ContactRequestsStore.MySuperStore = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.MySuperStore]
  }

  override def transform(value: ContactRequest): ContactRequest = {
    val updatedContact = Option(myStateStore.get(value.userId))
      .map { contact =>
        contact.copy(deduplicatedNumber = contact.deduplicatedNumber + 1)
      }
      .getOrElse(value)

    myStateStore.put(updatedContact.userId, updatedContact)

    if (updatedContact.isFresh) {
      value
    } else null
  }

  override def close(): Unit = {}
}

case class MyPunctuator(context: ProcessorContext, store: ContactRequestsStore.MySuperStore)
    extends Punctuator
    with LazyLogging {
  def punctuate(tstamp: Long): Unit = {
    val now = new DateTime(tstamp)
    logger.info(s"Executing punctuator ${now}")

    CloseableResource(store.all()) { record =>
      while (record.hasNext) {
        val contactRequest = record.next().value
        if (contactRequest.isReadyToSend(now)) {
          this.context.forward(contactRequest.userId, contactRequest)
        }
      }
    }
  }
}

class ScheduleContactRequests(
    val scheduleInterval: Int = 10000,
    initialScheduleSetter: (ContactRequest) => ContactRequest
) extends Transformer[String, ContactRequest, KeyValue[String, ContactRequest]]
    with LazyLogging {
  var myStateStore: ContactRequestsStore.MySuperStore = _
  var context: ProcessorContext                       = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.MySuperStore]
    this.context = context

    logger.info(s"Setting up schedule")

    context.schedule(
      time.Duration.ofMillis(scheduleInterval),
      PunctuationType.WALL_CLOCK_TIME,
      MyPunctuator(this.context, myStateStore)
    )
  }

  override def transform(key: String, value: ContactRequest): KeyValue[String, ContactRequest] = {
    myStateStore.put(value.userId, initialScheduleSetter(value))
    null
  }

  override def close(): Unit = {}
}

object TopologyWithSchedule extends LazyLogging {
  import Serdes._
  import StreamEnricher._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    import scala.concurrent.duration
    implicit val customerSerde       = Customer.serde
    implicit val rfqCreatedSerde     = RfqCreatedEvent.serde
    implicit val contactRequestSerde = ContactRequest.serde

    // setup store
    val storeSupplier = Stores.persistentKeyValueStore(ContactRequestsStore.name)
    val storeBuilder =
      Stores.keyValueStoreBuilder(storeSupplier, ContactRequestsStore.keySerde, ContactRequestsStore.valSerde)
    builder.addStateStore(storeBuilder)


    // source stream processors
    val customersTable = builder
      .globalTable[String, Customer](Topics.customerTopic)
    val rfqCreatedStream: KStream[String, RfqCreatedEvent] = builder
      .stream[String, RfqCreatedEvent](Topics.rfqCreateTopic)

    // creating a transformer that's a part of Processor API
    val deduplicationTransformer = new ValueTransformerSupplier[ContactRequest, ContactRequest] {
      override def get(): ValueTransformer[ContactRequest, ContactRequest] = new StoreAndDeduplicateContactRequests()
    }
    import scala.concurrent.duration._
    val scheduleSetter = (c: ContactRequest) => {
      c.copy(
        scheduledAt = Some(
          DateTime
            .now()
            .plusSeconds(50.seconds.toSeconds.toInt)
        )
      )
    }
    val scheduleTransformer = new TransformerSupplier[String, ContactRequest, KeyValue[String, ContactRequest]] {
      override def get(): Transformer[String, ContactRequest, KeyValue[String, ContactRequest]] =
        new ScheduleContactRequests(1000, scheduleSetter)
    }

    rfqCreatedStream
      .join(customersTable)(
        (_, rfqCreatedEvent) => rfqCreatedEvent.customerId,
        createContactRequest
      )
      .transformValues(deduplicationTransformer, ContactRequestsStore.name)
      .notNull
      .transform(scheduleTransformer, ContactRequestsStore.name)
      .notNull
      .to(Topics.contactRequests)
  }

  def createContactRequest(rfqCreatedEvent: RfqCreatedEvent, customer: Customer) =
    ContactRequest(
      customer.id,
      rfqCreatedEvent.decision,
      ContactDetails(customer.name, customer.telephoneNumber)
    )
}
