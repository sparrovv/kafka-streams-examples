package com.mwrobel.kafkastreams.example_commands

import com.mwrobel.kafkastreams.example_commands.state.ContactRequest
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.KeyValueStore

import java.time.Instant

case class ContactRequestSchedule(userId: String, scheduledAt: Instant, contactRequest: ContactRequest)
object ContactRequestsScheduleStore {
  val name = "contact_requests_schedule_store"

  val keySerde = Serdes.String
  val valSerde = ContactRequest.serde

  type ContactRequestsSchedules = KeyValueStore[String, ContactRequestSchedule]

  object Enrichments {
    implicit class ContactRequestScheduleStorageEnrichments(val store: ContactRequestsSchedules) {
      def upsert(value: ContactRequestSchedule) = {
        store.put(value.userId, value)
      }

      def foreach(f: ContactRequestSchedule => Unit): Unit = {
        val iterator = store.all

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
