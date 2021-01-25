package com.mwrobel.kafkastreams.example_commands.transformers

import com.mwrobel.kafkastreams.example_commands.ContactRequestsStore
import com.mwrobel.kafkastreams.example_commands.models.Commands.{
  CreateContactRequest,
  RemoveContactRequest,
  ScheduleContactRequest,
  StateChangeCommand,
  TriggerContactRequest
}
import com.mwrobel.kafkastreams.example_commands.state.ContactRequest
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext

case class StateChange(contactRequest: Option[ContactRequest], forward: Boolean)

object BusinessLogic extends LazyLogging {
  def transform(existingState: Option[ContactRequest], incomingCommand: StateChangeCommand): StateChange = {

    incomingCommand match {
      case c: CreateContactRequest if existingState.isEmpty =>
        StateChange(Some(ContactRequest(c)), true)

      case c: CreateContactRequest if existingState.isDefined =>
        val newState = existingState.get.copy(
          appliedCommands = existingState.get.appliedCommands ++ Seq(c)
        )
        StateChange(Some(newState), false)

      case c: TriggerContactRequest if existingState.isDefined =>
        val newState = existingState.get.copy(
          appliedCommands = existingState.get.appliedCommands ++ Seq(c),
          currentState = ContactRequest.triggered
        )

        StateChange(Some(newState), true)

      case c: ScheduleContactRequest if existingState.isDefined =>
        val newState = existingState.get.copy(
          appliedCommands = existingState.get.appliedCommands ++ Seq(c),
          currentState = ContactRequest.scheduled
        )

        StateChange(Some(newState), true)

      case c: TriggerContactRequest if existingState.isDefined =>
        val newState = existingState.get.copy(
          appliedCommands = existingState.get.appliedCommands ++ Seq(c),
          currentState = ContactRequest.triggered
        )

        StateChange(Some(newState), true)
      case c: RemoveContactRequest if existingState.isDefined =>
        val newState = existingState.get.copy(
          appliedCommands = existingState.get.appliedCommands ++ Seq(c),
          currentState = ContactRequest.deleted
        )

        StateChange(Some(newState), true)

      case c: ScheduleContactRequest if existingState.isEmpty => {
        logger.warn(s"No state anymore, tried this ${c}")
        StateChange(None, false)
      }

      case _ => {
        logger.warn(s"Got command that is not handled: ${incomingCommand}")
        StateChange(None, false)
      }
    }
  }
}

class AppLogicTransformer extends ValueTransformer[StateChangeCommand, Option[ContactRequest]] {
  var myStateStore: ContactRequestsStore.ContactRequests = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.ContactRequests]
  }

  override def transform(value: StateChangeCommand): Option[ContactRequest] = {
    val maybeContactRequest = Option(myStateStore.get(value.userId))

    val stateChangeInfo: StateChange = BusinessLogic.transform(maybeContactRequest, value)

    // persist
    stateChangeInfo.contactRequest.foreach { contactRequest =>
      myStateStore.put(contactRequest.userId, contactRequest)
    }

    // forward down the stream
    if (stateChangeInfo.forward) {
      stateChangeInfo.contactRequest
    } else None
  }

  override def close(): Unit = {}
}
