package com.mwrobel.kafkastreams.example4.models

import com.mwrobel.kafkastreams.example4.serdes.JsonSerde
import org.joda.time.DateTime

trait DomainEvent
trait DomainEntity

case class Customer(id: String, name: String, telephoneNumber: String) extends DomainEntity
object Customer {
  type TelephoneNumber = String
  val serde: JsonSerde[Customer] = new JsonSerde[Customer]()
}

case class RfqCreatedEvent(
    eventId: String,
    rfqReference: String,
    customerId: String,
    quotesNumber: Int,
    decision: String
) extends DomainEvent
object RfqCreatedEvent {
  val serde: JsonSerde[RfqCreatedEvent] = new JsonSerde[RfqCreatedEvent]()
}
