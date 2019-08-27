package com.mwrobel.kafkastreams.example6.transformers

import java.time
import com.mwrobel.kafkastreams.example6.models.ContactRequest
import com.mwrobel.kafkastreams.example6.{ContactRequestsStore}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.joda.time.DateTime

case class ScheduleContactRequests(scheduleInterval: Int = 10000, setScheduledAt: (ContactRequest) => ContactRequest)
    extends Transformer[String, ContactRequest, KeyValue[String, Option[ContactRequest]]]
    with LazyLogging {
  var contactRequestsStore: ContactRequestsStore.ContactRequests = _

  import ContactRequestsStore.Enrichments._

  override def init(context: ProcessorContext): Unit = {
    contactRequestsStore =
      context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.ContactRequests]

    context.schedule(
      time.Duration.ofMillis(scheduleInterval),
      PunctuationType.WALL_CLOCK_TIME,
      (timestamp: Long) => {
        val now = new DateTime(timestamp)

        contactRequestsStore.foreach { contactRequest =>
          if (contactRequest.isReadyToSend(now)) {
            val updatedContactRequest = contactRequest.copy(forwarded = true)
            contactRequestsStore.upsert(updatedContactRequest)

            context.forward(updatedContactRequest.userId, Some(updatedContactRequest))
          }
        }
      }
    )
  }

  override def transform(key: String, value: ContactRequest): KeyValue[String, Option[ContactRequest]] = {
    contactRequestsStore.upsert(setScheduledAt(value))

    new KeyValue("x", None)
  }

  override def close(): Unit = {}
}
