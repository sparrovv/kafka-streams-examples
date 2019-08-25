package com.mwrobel.kafkastreams.example5

import com.mwrobel.kafkastreams.example5.models._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

object Topics {
  val contactDetails  = "contact_details"
  val contactRequests = "contact_requests"
  val quotesCreated   = "quotes_created"
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
    implicit val contactDetailsSerde = ContactDetailsEntity.serde
    implicit val quotesCreatedSerde  = QuotesCreated.serde
    implicit val contactRequestSerde = ContactRequest.serde

    // setup store
    val storeSupplier = Stores.persistentKeyValueStore(ContactRequestsStore.name)
    val storeBuilder =
      Stores.keyValueStoreBuilder(storeSupplier, ContactRequestsStore.keySerde, ContactRequestsStore.valSerde)
    builder.addStateStore(storeBuilder)

    // source stream processors
    val contactDetailsTable = builder
      .globalTable[String, ContactDetailsEntity](Topics.contactDetails)
    val quotesCreatedStream = builder
      .stream[String, QuotesCreated](Topics.quotesCreated)

    // creating a transformer that's a part of Processor API
    val deduplicationTransformer = new ValueTransformerSupplier[ContactRequest, ContactRequest] {
      override def get(): ValueTransformer[ContactRequest, ContactRequest] = new StoreAndDeduplicateContactRequests()
    }

    quotesCreatedStream
      .join(contactDetailsTable)(
        (_, quotesCreated) => quotesCreated.userId,
        createContactRequest
      )
      .transformValues(deduplicationTransformer, ContactRequestsStore.name)
      .filter((_, v) => v != null)
      .to(Topics.contactRequests)
  }

  def createContactRequest(quotesCreated: QuotesCreated, contactDetailsEntity: ContactDetailsEntity) =
    ContactRequest(
      quotesCreated.userId,
      quotesCreated.quotesNumber,
      quotesCreated.reference,
      ContactDetails(contactDetailsEntity.name, contactDetailsEntity.telephoneNumber)
    )
}
