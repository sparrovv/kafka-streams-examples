package com.mwrobel.kafkastreams.example6

import java.time

import com.mwrobel.kafkastreams.LeadManagementTopics
import com.mwrobel.kafkastreams.example6.models._
import com.mwrobel.kafkastreams.example6.transformers.{ScheduleContactRequests, StoreAndDeduplicateContactRequests}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier, ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.joda.time.{DateTime, DateTimeZone}
import scala.concurrent.duration._

object ContactRequestsStore {
  val name = "contact_requests_store"

  val keySerde = Serdes.String
  val valSerde = ContactRequest.serde

  type ContactRequests = KeyValueStore[String, ContactRequest]
}

object TopologyWithSchedule extends LazyLogging {
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
      .globalTable[String, ContactDetailsEntity](LeadManagementTopics.contactDetailsEntity)
    val quotesCreatedStream = builder
      .stream[String, QuotesCreated](LeadManagementTopics.quotesCreated)

    // creating a transformer that's a part of Processor API
    val saveAndDeduplicate = new ValueTransformerSupplier[ContactRequest, ContactRequest] {
      override def get(): ValueTransformer[ContactRequest, ContactRequest] = new StoreAndDeduplicateContactRequests()
    }

    val scheduleTransformer = new TransformerSupplier[String, ContactRequest, KeyValue[String, ContactRequest]] {
      override def get(): Transformer[String, ContactRequest, KeyValue[String, ContactRequest]] =
        new ScheduleContactRequests(1000, contactRequestScheduleSetter)
    }

    quotesCreatedStream
      .join(contactDetailsTable)(
        (_, quotesCreatedEvent) => quotesCreatedEvent.userId,
        createContactRequest
      )
      .transformValues(saveAndDeduplicate, ContactRequestsStore.name)
      .filter((_, v) => v != null)
      .transform(scheduleTransformer, ContactRequestsStore.name)
      .filter((_, v) => v != null)
      .to(LeadManagementTopics.contactRequests)
  }

  def contactRequestScheduleSetter(c: ContactRequest): ContactRequest = {
    c.copy(
      scheduledAt = Some(
        DateTime
          .now()
          .plusSeconds(50.seconds.toSeconds.toInt)
      )
    )
  }

  def createContactRequest(quotesCreatedEvent: QuotesCreated, contactDetails: ContactDetailsEntity) =
    ContactRequest(
      quotesCreatedEvent.userId,
      quotesCreatedEvent.quotesNumber,
      quotesCreatedEvent.reference,
      ContactDetails(contactDetails.name, contactDetails.telephoneNumber)
    )
}
