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

class ScheduleContactRequests(
    val scheduleInterval: Int = 10000,
    initialScheduleSetter: (ContactRequest) => ContactRequest
) extends Transformer[String, ContactRequest, KeyValue[String, ContactRequest]]
    with LazyLogging {
  var myStateStore: ContactRequestsStore.ContactRequests = _
  var context: ProcessorContext                          = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.ContactRequests]
    this.context = context

    this.context.schedule(
      time.Duration.ofMillis(scheduleInterval),
      PunctuationType.WALL_CLOCK_TIME,
      (tstamp: Long) => {
        val now = new DateTime(tstamp)

        CloseableResource(myStateStore.all()) { record =>
          while (record.hasNext) {
            val contactRequest = record.next().value

            if (contactRequest.isReadyToSend(now)) {
              this.context.forward(contactRequest.userId, contactRequest)
            }
          }
        }
      }
    )
  }

  override def transform(key: String, value: ContactRequest): KeyValue[String, ContactRequest] = {
    myStateStore.put(value.userId, initialScheduleSetter(value))

    null
  }

  override def close(): Unit = {}
}
