package com.mwrobel.kafkastreams.example4.models

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

object ContactDetailsEntity {
  val serde: JsonSerde[ContactDetailsEntity] = new JsonSerde[ContactDetailsEntity]()
}

object QuotesCreated {
  val serde: JsonSerde[QuotesCreated] = new JsonSerde[QuotesCreated]()
}
