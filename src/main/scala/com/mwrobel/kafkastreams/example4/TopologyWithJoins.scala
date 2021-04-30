package com.mwrobel.kafkastreams.example4

import com.mwrobel.kafkastreams.LeadManagementTopics
import com.mwrobel.kafkastreams.example4.models._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

object TopologyWithStateStore extends LazyLogging {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    implicit val contactDetailsSerde = ContactDetailsEntity.serde
    implicit val quotesCreatedSerde  = QuotesCreated.serde
    implicit val contactRequestSerde = ContactRequest.serde

    val quotesCreated = builder
      .stream[String, QuotesCreated](LeadManagementTopics.quotesCreated)

    val contactDetails = builder
      .globalTable[String, ContactDetailsEntity](LeadManagementTopics.contactDetailsEntity)

    quotesCreated
      .join(contactDetails)(
        (_, quotesCreated) => quotesCreated.userId,
        createContactRequest
      )
      .to(LeadManagementTopics.contactRequests)
  }

  def createContactRequest(quotesCreated: QuotesCreated, contactDetails: ContactDetailsEntity) =
    ContactRequest(
      contactDetails.id,
      quotesCreated.quotesNumber,
      quotesCreated.reference,
      ContactDetails(contactDetails.name, contactDetails.telephoneNumber)
    )
}
