package com.mwrobel.kafkastreams.example5.transformers

import com.mwrobel.kafkastreams.example5.ContactRequestsStore
import com.mwrobel.kafkastreams.example5.models.ContactRequest
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext

class StoreAndDeduplicateContactRequests extends ValueTransformer[ContactRequest, ContactRequest] {
  var contactRequestStore: ContactRequestsStore.ContactRequests = _

  override def init(context: ProcessorContext): Unit = {
    contactRequestStore =
      context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.ContactRequests]
  }

  override def transform(value: ContactRequest): ContactRequest = {
    val updatedContact = Option(contactRequestStore.get(value.userId))
      .map { contact =>
        contact.copy(deduplicatedNumber = contact.deduplicatedNumber + 1)
      }
      .getOrElse(value)

    contactRequestStore.put(updatedContact.userId, updatedContact)

    if (updatedContact.isFresh) {
      value
    } else null
  }

  override def close(): Unit = {}
}
