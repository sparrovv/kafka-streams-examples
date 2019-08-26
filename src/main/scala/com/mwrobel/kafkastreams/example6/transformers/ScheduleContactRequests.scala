package com.mwrobel.kafkastreams.example6.transformers

import java.time
import com.mwrobel.kafkastreams.example6.models.ContactRequest
import com.mwrobel.kafkastreams.example6.{ContactRequestsStore}
import com.mwrobel.kafkastreams.utils.CloseableResource
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.joda.time.DateTime

case class ScheduleContactRequests(scheduleInterval: Int = 10000, scheduledAtSetter: (ContactRequest) => ContactRequest)
    extends Transformer[String, ContactRequest, KeyValue[String, ContactRequest]]
    with LazyLogging {
  var contactRequestsStore: ContactRequestsStore.ContactRequests = _

  override def init(context: ProcessorContext): Unit = {
    contactRequestsStore =
      context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.ContactRequests]

    context.schedule(
      time.Duration.ofMillis(scheduleInterval),
      PunctuationType.WALL_CLOCK_TIME,
      (timestamp: Long) => {
        val now = new DateTime(timestamp)

        CloseableResource(contactRequestsStore.all()) { record =>
          while (record.hasNext) {
            val contactRequest = record.next().value

            if (contactRequest.isReadyToSend(now)) {
              val updatedContactRequest = contactRequest.copy(forwarded = true)
              contactRequestsStore.put(updatedContactRequest.userId, updatedContactRequest)

              context.forward(updatedContactRequest.userId, updatedContactRequest)
            }
          }
        }
      }
    )
  }

  override def transform(key: String, value: ContactRequest): KeyValue[String, ContactRequest] = {
    contactRequestsStore.put(value.userId, scheduledAtSetter(value))

    null
  }

  override def close(): Unit = {}
}
