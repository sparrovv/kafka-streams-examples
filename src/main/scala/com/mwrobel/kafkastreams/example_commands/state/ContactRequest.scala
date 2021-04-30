package com.mwrobel.kafkastreams.example_commands.state

import com.mwrobel.kafkastreams.example_commands.models.Commands.{CreateContactRequest, StateChangeCommand}
import com.mwrobel.kafkastreams.serdes.JsonSerde

import java.time.Instant

case class ContactDetails(name: String, telephoneNumber: String)

case class ContactRequestInitState(
    id: String,
    userId: String,
    quotesNumber: Int,
    reference: String,
    contactDetails: Option[ContactDetails] = None,
    causationId: String,
    causationEventName: String
)

object ContactRequestInitState {
  val serde: JsonSerde[ContactRequestInitState] = new JsonSerde[ContactRequestInitState]()
}

case class ContactRequest(
    id: String,
    userId: String,
    quotesNumber: Int,
    reference: String,
    currentState: String,
    createdAt: Instant,
    contactDetails: Option[ContactDetails] = None,
    leadScored: Option[Double] = None,
    appliedCommands: Seq[StateChangeCommand] = Seq()
)

object ContactRequest {
  val serde: JsonSerde[ContactRequest] = new JsonSerde[ContactRequest]()

  // possible states
  val created     = "created"
  val triggered   = "triggered"
  val scheduled   = "scheduled"
  val deleted     = "deleted"
  val prioritized = "prioritized"

  def apply(create: CreateContactRequest): ContactRequest = {
    ContactRequest(
      id = create.causationId,
      userId = create.userId,
      createdAt = Instant.now(), //@todo defer
      quotesNumber = create.contactRequestInitState.quotesNumber,
      reference = create.contactRequestInitState.reference,
      contactDetails = create.contactRequestInitState.contactDetails,
      currentState = created,
      appliedCommands = Seq(create)
    )
  }
}
