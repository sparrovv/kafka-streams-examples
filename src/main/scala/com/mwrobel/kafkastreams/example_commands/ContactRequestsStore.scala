package com.mwrobel.kafkastreams.example_commands

import com.mwrobel.kafkastreams.example_commands.state.ContactRequest
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.KeyValueStore

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
