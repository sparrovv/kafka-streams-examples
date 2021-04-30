package com.mwrobel.kafkastreams.example_commands.models

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

case class PolicyPurchased(
    eventId: String,
    policyId: String,
    userId: String
) extends DomainEvent

case class ContactRequestCreated(
    userId: String
) extends DomainEvent

case class ContactRequestPrioritized(
    userId: String
) extends DomainEvent

case class ContactRequestRemoved(
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

object PolicyPurchased {
  val serde: JsonSerde[PolicyPurchased] = new JsonSerde[PolicyPurchased]()
}

object ContactRequestCreated {
  val serde: JsonSerde[ContactRequestCreated] = new JsonSerde[ContactRequestCreated]()
}
object ContactRequestPrioritized {
  val serde: JsonSerde[ContactRequestPrioritized] = new JsonSerde[ContactRequestPrioritized]()
}
object ContactRequestRemoved {
  val serde: JsonSerde[ContactRequestRemoved] = new JsonSerde[ContactRequestRemoved]()
}
