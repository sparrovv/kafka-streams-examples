package com.mwrobel.kafkastreams.example4

import com.mwrobel.kafkastreams.example4.models._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

object Topics {
  val customerTopic   = "customers"
  val contactRequests = "contact_requests"
  val rfqCreateTopic  = "rfq_created"
}

object TopologyWithStateStore extends LazyLogging {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    implicit val customerSerde       = Customer.serde
    implicit val rfqCreatedSerde     = RfqCreatedEvent.serde
    implicit val contactRequestSerde = ContactRequest.serde

    val customersTable = builder
      .globalTable[String, Customer](Topics.customerTopic)
    val rfqCreatedStream = builder
      .stream[String, RfqCreatedEvent](Topics.rfqCreateTopic)

    rfqCreatedStream
      .join(customersTable)(
        (_, rfqCreatedEvent) => rfqCreatedEvent.customerId,
        createContactRequest
      )
      .to(Topics.contactRequests)
  }

  def createContactRequest(rfqCreatedEvent: RfqCreatedEvent, customer: Customer) =
    ContactRequest(
      customer.id,
      rfqCreatedEvent.decision,
      ContactDetails(customer.name, customer.telephoneNumber)
    )
}
