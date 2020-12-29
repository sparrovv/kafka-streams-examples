package com.mwrobel.kafkastreams.example_copartioning

import com.mwrobel.kafkastreams.LeadManagementTopics
import com.mwrobel.kafkastreams.example_copartioning.models._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

import java.util.UUID
import java.util.concurrent.TimeUnit

object Topics {
  val contactDetailsEntity_2p = "contact_details_2p"
  val leadScored              = "lead_scored_2p"
  val leadActioned            = "lead_actioned_2p"
  val contactRequests         = "contact_requests"
  val quotesCreated_3p        = "quotes_created_3p"
}

object TopologyWithJoins extends LazyLogging {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    implicit val contactDetailsSerde = ContactDetailsEntity.serde
    implicit val quotesCreatedSerde  = QuotesCreated.serde
    implicit val contactRequestSerde = ContactRequest.serde

    val quotesCreated = builder
      .stream[String, QuotesCreated](Topics.quotesCreated_3p)
      .selectKey((_, v) => v.userId)

    val leadScored = builder
      .stream[String, QuotesCreated](Topics.leadScored)
      .selectKey((_, v) => v.userId)

    val leadActioned = builder
      .stream[String, QuotesCreated](Topics.leadActioned)
      .selectKey((_, v) => v.userId)

    val contactDetails = builder
      .stream[String, ContactDetailsEntity](Topics.contactDetailsEntity_2p)
      .selectKey((_, v) => v.id)

    quotesCreated
      .join(contactDetails)(
        createContactRequest,
        JoinWindows.of(TimeUnit.MINUTES.toMillis(5))
      )
      .to(LeadManagementTopics.contactRequests)
  }

  def createContactRequest(quotesCreated: QuotesCreated, contactDetails: ContactDetailsEntity) =
    ContactRequest(
      id = UUID.randomUUID.toString(),
      userId = contactDetails.id,
      quotesCreated.quotesNumber,
      quotesCreated.reference,
      ContactDetails(contactDetails.name, contactDetails.telephoneNumber)
    )
}
