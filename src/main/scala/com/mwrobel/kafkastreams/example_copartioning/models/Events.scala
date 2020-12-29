package com.mwrobel.kafkastreams.example_copartioning.models

import com.mwrobel.kafkastreams.serdes.JsonSerde

trait DomainEvent
trait DomainEntity

case class ContactDetailsEntity(id: String, name: String, telephoneNumber: String) extends DomainEntity

case class QuotesCreated(
    eventId: String,
    reference: String,
    userId: String,
    quotesNumber: Int
) extends DomainEvent

case class LeadScored(
    leadScore: Int,
    userId: String
) extends DomainEvent

case class LeadActioned(
    action: String,
    userId: String
) extends DomainEvent

object ContactDetailsEntity {
  val serde: JsonSerde[ContactDetailsEntity] = new JsonSerde[ContactDetailsEntity]()
}

object QuotesCreated {
  val serde: JsonSerde[QuotesCreated] = new JsonSerde[QuotesCreated]()
}

object LeadActioned {
  val serde: JsonSerde[LeadActioned] = new JsonSerde[LeadActioned]()
}
object LeadScored {
  val serde: JsonSerde[LeadScored] = new JsonSerde[LeadScored]()
}
