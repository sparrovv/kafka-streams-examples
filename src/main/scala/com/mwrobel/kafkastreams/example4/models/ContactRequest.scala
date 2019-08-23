package com.mwrobel.kafkastreams.example4.models

import com.mwrobel.kafkastreams.example4.serdes.JsonSerde
import org.joda.time.DateTime

case class ContactDetails(name: String, telephoneNumber: String)
case class ContactRequest(
    userId: String,
    rfqDecision: String,
    contactDetails: ContactDetails,
    scheduledAt: Option[DateTime] = None
)

object ContactRequest {
  val serde: JsonSerde[ContactRequest] = new JsonSerde[ContactRequest]()
}
