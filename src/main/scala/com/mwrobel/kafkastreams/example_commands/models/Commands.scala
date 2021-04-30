package com.mwrobel.kafkastreams.example_commands.models

import com.mwrobel.kafkastreams.example_commands.state.{ContactRequest, ContactRequestInitState}
import com.mwrobel.kafkastreams.serdes.JsonSerde

import java.time.Instant
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object Commands {
  sealed trait StateChangeCommand {
    def userId: String
    def eventName: String
    def causationId: String
    def causationEventName: String
  }

  class CommandsSerde[T <: StateChangeCommand: TypeTag: ClassTag] {
    def serde: JsonSerde[T] = new JsonSerde[T]()
  }

  val allCommandsSerdes = Map(
    "remove_contact_request" -> RemoveContactRequest.serde,
    "create_contact_request" -> CreateContactRequest.serde
  )

  case class RemoveContactRequest(
      userId: String,
      causationEventName: String,
      causationId: String,
      lateArrivingId: Option[String] = None
  ) extends StateChangeCommand {
    val eventName = "remove_contact_request"
  }
  object RemoveContactRequest extends CommandsSerde[RemoveContactRequest]

  case class CreateContactRequest(
      causationEventName: String,
      causationId: String,
      userId: String,
      contactRequestInitState: ContactRequestInitState
  ) extends StateChangeCommand {
    val eventName = "create_contact_request"
  }

  object CreateContactRequest extends CommandsSerde[CreateContactRequest]

  case class PrioritizeContactRequest(
      causationEventName: String,
      causationId: String,
      userId: String,
      score: Double
  ) extends StateChangeCommand {
    val eventName = "prioritize_contact_request"
  }
  object PrioritizeContactRequest extends CommandsSerde[PrioritizeContactRequest]

  // This is a special backdoor command in case we need to fix something / migrate
  case class ReplaceContactRequest(
      causationEventName: String,
      causationId: String,
      userId: String,
      state: ContactRequest
  ) extends StateChangeCommand {
    val eventName = "replace_contact_request"
  }

  object ReplaceContactRequest extends CommandsSerde[ReplaceContactRequest]

  case class TriggerContactRequest(
      causationEventName: String,
      causationId: String,
      userId: String,
      state: ContactRequest
  ) extends StateChangeCommand {
    val eventName = "trigger_contact_request"
  }

  object TriggerContactRequest extends CommandsSerde[TriggerContactRequest]

  case class ScheduleContactRequest(
      causationEventName: String,
      causationId: String,
      userId: String,
      scheduledAt: Instant,
      state: ContactRequest
  ) extends StateChangeCommand {
    val eventName = "trigger_contact_request"
  }

  object ScheduleContactRequest extends CommandsSerde[ScheduleContactRequest]
}
