package com.mwrobel.kafkastreams.example6

import com.mwrobel.kafkastreams.LeadManagementTopics
import com.mwrobel.kafkastreams.example6.models._
import com.mwrobel.kafkastreams.example6.transformers.{ScheduleContactRequests, StoreAndDeduplicateContactRequests}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier, ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.joda.time.{DateTime}

import scala.concurrent.duration._

object StreamEnricher {
  implicit def toStreamEnricher[K, V](ks: KStream[K, V]) = new StreamEnricher(ks)

  class StreamEnricher[K, V](kstream: KStream[K, V]) {
    def notNull: KStream[K, V] = {
      kstream.filter((_, v) => v != null)
    }
  }
}

object ContactRequestsStore {
  val name = "contact_requests_store"

  val keySerde = Serdes.String
  val valSerde = ContactRequest.serde

  type ContactRequests = KeyValueStore[String, ContactRequest]

  object Enrichments {
    implicit class ContactRequestScheduleStorageEnrichments(val contactRequestsStore: ContactRequests) {
      def upsert(contactRequest: ContactRequest) = {
        contactRequestsStore.put(contactRequest.userId, contactRequest)
      }

      def foreach(f: ContactRequest => Unit): Unit = {
        val iterator = contactRequestsStore.all

        try {
          while (iterator.hasNext) {
            f(iterator.next.value)
          }
        } finally {
          iterator.close()
        }
      }
    }
  }

}

object TopologyWithSchedule extends LazyLogging {
  import Serdes._
  import StreamEnricher._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    implicit val contactDetailsSerde = ContactDetailsEntity.serde
    implicit val quotesCreatedSerde  = QuotesCreated.serde
    implicit val contactRequestSerde = ContactRequest.serde

    val contactDetailsTable = builder
      .globalTable[String, ContactDetailsEntity](LeadManagementTopics.contactDetailsEntity)
    val quotesCreatedStream = builder
      .stream[String, QuotesCreated](LeadManagementTopics.quotesCreated)

    val storeSupplier = Stores.persistentKeyValueStore(ContactRequestsStore.name)
    val storeBuilder =
      Stores.keyValueStoreBuilder(storeSupplier, ContactRequestsStore.keySerde, ContactRequestsStore.valSerde)
    builder.addStateStore(storeBuilder)

    val saveAndDeduplicate = new ValueTransformerSupplier[ContactRequest, Option[ContactRequest]] {
      override def get(): ValueTransformer[ContactRequest, Option[ContactRequest]] =
        new StoreAndDeduplicateContactRequests()
    }

    val delayAndSchedule = new TransformerSupplier[String, ContactRequest, KeyValue[String, Option[ContactRequest]]] {
      override def get(): Transformer[String, ContactRequest, KeyValue[String, Option[ContactRequest]]] =
        new ScheduleContactRequests(1000, contactRequestScheduleSetter)
    }

    quotesCreatedStream
      .join(contactDetailsTable)(
        (_, quotesCreatedEvent) => quotesCreatedEvent.userId,
        createContactRequest
      )
      .transformValues(saveAndDeduplicate, ContactRequestsStore.name)
      .flatMapValues(v => v)
      .transform(delayAndSchedule, ContactRequestsStore.name)
      .flatMapValues(v => v)
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
