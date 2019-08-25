package com.mwrobel.kafkastreams.example6.models

import com.mwrobel.kafkastreams.serdes.JsonSerde
import org.joda.time.DateTime

trait DomainEvent
trait DomainEntity

case class ContactDetailsEntity(id: String, name: String, telephoneNumber: String) extends DomainEntity
object ContactDetailsEntity {
  type TelephoneNumber = String
  val serde: JsonSerde[ContactDetailsEntity] = new JsonSerde[ContactDetailsEntity]()
}

case class QuotesCreated(
    eventId: String,
    reference: String,
    userId: String,
    quotesNumber: Int
) extends DomainEvent
object QuotesCreated {
  val serde: JsonSerde[QuotesCreated] = new JsonSerde[QuotesCreated]()
}
