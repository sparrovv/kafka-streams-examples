package com.mwrobel.kafkastreams.example_copartioning.models

import com.mwrobel.kafkastreams.serdes.JsonSerde

case class ContactDetails(name: String, telephoneNumber: String)
case class ContactRequest(
    id: String,
    userId: String,
    quotesNumber: Int,
    reference: String,
    contactDetails: ContactDetails
)

object ContactRequest {
  val serde: JsonSerde[ContactRequest] = new JsonSerde[ContactRequest]()
}
