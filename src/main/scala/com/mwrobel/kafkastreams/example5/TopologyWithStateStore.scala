package com.mwrobel.kafkastreams.example5

import com.mwrobel.kafkastreams.LeadManagementTopics
import com.mwrobel.kafkastreams.example5.models._
import com.mwrobel.kafkastreams.example5.transformers.StoreAndDeduplicateContactRequests
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

object ContactRequestsStore {
  val name = "contact_requests"

  val keySerde = Serdes.String
  val valSerde = ContactRequest.serde

  type ContactRequests = KeyValueStore[String, ContactRequest]
}

object TopologyWithStateStore extends LazyLogging {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    implicit val contactDetailsSerde = ContactDetailsEntity.serde
    implicit val quotesCreatedSerde  = QuotesCreated.serde
    implicit val contactRequestSerde = ContactRequest.serde

    val contactDetailsTable = builder
      .globalTable[String, ContactDetailsEntity](LeadManagementTopics.contactDetailsEntity)
    val quotesCreatedStream = builder
      .stream[String, QuotesCreated](LeadManagementTopics.quotesCreated)

    val storeSupplier: KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(ContactRequestsStore.name)
    val storeBuilder: StoreBuilder[KeyValueStore[String, ContactRequest]] = Stores.keyValueStoreBuilder(
      storeSupplier,
      ContactRequestsStore.keySerde,
      ContactRequestsStore.valSerde
    )
    builder.addStateStore(storeBuilder)

    val saveAndDeduplicate = new ValueTransformerSupplier[ContactRequest, ContactRequest] {
      override def get(): ValueTransformer[ContactRequest, ContactRequest] = new StoreAndDeduplicateContactRequests()
    }

    quotesCreatedStream
      .join(contactDetailsTable)(
        (_, quotesCreated) => quotesCreated.userId,
        createContactRequest
      )
      .transformValues(saveAndDeduplicate, ContactRequestsStore.name)
      .filter((_, v) => v != null)
      .to(LeadManagementTopics.contactRequests)
  }

  def createContactRequest(quotesCreated: QuotesCreated, contactDetailsEntity: ContactDetailsEntity) =
    ContactRequest(
      quotesCreated.userId,
      quotesCreated.quotesNumber,
      quotesCreated.reference,
      ContactDetails(contactDetailsEntity.name, contactDetailsEntity.telephoneNumber)
    )
}
