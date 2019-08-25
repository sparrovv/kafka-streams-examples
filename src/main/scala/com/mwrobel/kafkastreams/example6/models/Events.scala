package com.mwrobel.kafkastreams.example6.models

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.mwrobel.kafkastreams.example6.serdes.JsonSerde
import org.joda.time.DateTime

trait DomainEvent
trait DomainEntity

case class Customer(id: String, name: String, telephoneNumber: String) extends DomainEntity
object Customer {
  type TelephoneNumber = String
  val serde: JsonSerde[Customer] = new JsonSerde[Customer]()
}

object Decision extends Enumeration {
  type Decision = Value
  val Quoted = Value("quoted")
  val Referred = Value("referred")
}
class DecisionType extends TypeReference[Decision.type]

case class RfqCreatedEvent(
    eventId: String,
    rfqReference: String,
    customerId: String,
    quotesNumber: Int,
    @JsonScalaEnumeration(classOf[DecisionType]) decision: Decision.Decision
) extends DomainEvent
object RfqCreatedEvent {
  val serde: JsonSerde[RfqCreatedEvent] = new JsonSerde[RfqCreatedEvent]()
}
