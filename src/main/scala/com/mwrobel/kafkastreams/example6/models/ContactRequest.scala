package com.mwrobel.kafkastreams.example6.models

import com.mwrobel.kafkastreams.serdes.JsonSerde
import org.joda.time.DateTime

case class ContactDetails(name: String, telephoneNumber: String)
case class ContactRequest(
    userId: String,
    quotesNumber: Int,
    reference: String,
    contactDetails: ContactDetails,
    deduplicatedNumber: Int = 0,
    scheduledAt: Option[DateTime] = None,
    forwarded: Boolean = false
) {
  def isFresh: Boolean                      = deduplicatedNumber == 0
  def isReadyToSend(now: DateTime): Boolean = !forwarded && scheduledAt.map(_.isBefore(now.toInstant)).getOrElse(false)
}

object ContactRequest {
  val serde: JsonSerde[ContactRequest] = new JsonSerde[ContactRequest]()
}
