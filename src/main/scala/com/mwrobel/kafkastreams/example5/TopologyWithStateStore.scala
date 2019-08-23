package com.mwrobel.kafkastreams.example5

import com.mwrobel.kafkastreams.example5.models._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

object Topics {
  val customerTopic   = "customers"
  val contactRequests = "contact_requests"
  val rfqCreateTopic  = "rfq_created"
}

object ContactRequestsStore {
  val name = "contact_requests"

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

object TopologyWithStateStore extends LazyLogging {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
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
    val rfqCreatedStream = builder
      .stream[String, RfqCreatedEvent](Topics.rfqCreateTopic)

    // creating a transformer that's a part of Processor API
    val transformSupplier = new ValueTransformerSupplier[ContactRequest, ContactRequest] {
      override def get(): ValueTransformer[ContactRequest, ContactRequest] = new StoreAndDeduplicateContactRequests()
    }

    rfqCreatedStream
      .join(customersTable)(
        (_, rfqCreatedEvent) => rfqCreatedEvent.customerId,
        createContactRequest
      )
      .transformValues(transformSupplier, ContactRequestsStore.name)
      .filter((_, v) => v != null) // can I make it generic?
      .to(Topics.contactRequests)
  }

  def createContactRequest(rfqCreatedEvent: RfqCreatedEvent, customer: Customer) =
    ContactRequest(
      customer.id,
      rfqCreatedEvent.decision,
      ContactDetails(customer.name, customer.telephoneNumber)
    )
}
