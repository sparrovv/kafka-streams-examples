package com.mwrobel.kafkastreams.example6.models

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.mwrobel.kafkastreams.example6.serdes.JsonSerde
import org.joda.time.DateTime

case class ContactDetails(name: String, telephoneNumber: String)
case class ContactRequest(
    userId: String,
    @JsonScalaEnumeration(classOf[DecisionType]) rfqDecision: Decision.Decision,
    contactDetails: ContactDetails,
    scheduledAt: Option[DateTime] = None,
    deduplicatedNumber: Int = 0
) {

  def isFresh: Boolean                      = deduplicatedNumber == 0
  def isReadyToSend(now: DateTime): Boolean = scheduledAt.map(_.isBefore(now.toInstant)).getOrElse(false)
}

object ContactRequest {
  val serde: JsonSerde[ContactRequest] = new JsonSerde[ContactRequest]()
}
