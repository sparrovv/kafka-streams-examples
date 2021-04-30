package com.mwrobel.kafkastreams.example_commands.transformers

import com.mwrobel.kafkastreams.example_commands.{ContactRequestSchedule, ContactRequestsScheduleStore}
import com.mwrobel.kafkastreams.example_commands.models.Commands.{
  ScheduleContactRequest,
  StateChangeCommand,
  TriggerContactRequest
}
import com.mwrobel.kafkastreams.example_commands.state.ContactRequest
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}

import java.time
import java.time.Instant

case class ScheduleContactRequests(scheduleInterval: Int = 10000, setScheduledAt: (ContactRequest) => Instant)
    extends Transformer[String, ContactRequest, KeyValue[String, StateChangeCommand]]
    with LazyLogging {
  var store: ContactRequestsScheduleStore.ContactRequestsSchedules = _
  import ContactRequestsScheduleStore.Enrichments._

  override def init(context: ProcessorContext): Unit = {
    store = context
      .getStateStore(ContactRequestsScheduleStore.name)
      .asInstanceOf[ContactRequestsScheduleStore.ContactRequestsSchedules]

    context.schedule(
      time.Duration.ofMillis(scheduleInterval),
      PunctuationType.WALL_CLOCK_TIME,
      (timestamp: Long) => {
        store.foreach { scheduleState =>
          if (scheduleState.scheduledAt.isAfter(Instant.ofEpochMilli(timestamp))) {

            val trigger = TriggerContactRequest(
              causationEventName = "internal_scheduler",
              causationId = s"${scheduleState.userId}-${timestamp}",
              userId = scheduleState.userId,
              state = scheduleState.contactRequest
            )

            context.forward(scheduleState.userId, trigger)

            store.delete(scheduleState.userId)
          }
        }
      }
    )
  }

  override def transform(key: String, value: ContactRequest): KeyValue[String, StateChangeCommand] = {
    val scheduleAt = setScheduledAt(value)

    store.upsert(ContactRequestSchedule(value.userId, scheduleAt, value))

    new KeyValue(
      value.userId,
      ScheduleContactRequest(
        userId = value.userId,
        scheduledAt = scheduleAt,
        state = value,
        causationEventName = "system-event",
        causationId = s"${value.userId}-${scheduleAt}"
      )
    )
  }

  override def close(): Unit = {}
}
