package com.mwrobel.kafkastreams.example5.models

import com.mwrobel.kafkastreams.serdes.JsonSerde
import org.joda.time.DateTime

case class ContactDetails(name: String, telephoneNumber: String)
case class ContactRequest(
    userId: String,
    quotesNumber: Int,
    reference: String,
    contactDetails: ContactDetails,
    deduplicatedNumber: Int = 0
) {

  def isFresh: Boolean = deduplicatedNumber == 0
}

object ContactRequest {
  val serde: JsonSerde[ContactRequest] = new JsonSerde[ContactRequest]()
}
