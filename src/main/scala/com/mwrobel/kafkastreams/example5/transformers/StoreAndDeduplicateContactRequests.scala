package com.mwrobel.kafkastreams.example5.transformers

import com.mwrobel.kafkastreams.example5.ContactRequestsStore
import com.mwrobel.kafkastreams.example5.models.ContactRequest
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext

class StoreAndDeduplicateContactRequests extends ValueTransformer[ContactRequest, Option[ContactRequest]] {
  var myStateStore: ContactRequestsStore.ContactRequests = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(ContactRequestsStore.name)
      .asInstanceOf[ContactRequestsStore.ContactRequests]
  }

  override def transform(value: ContactRequest): Option[ContactRequest] = {
    val updatedContact = Option(myStateStore.get(value.userId))
      .map { contact =>
        contact.copy(deduplicatedNumber = contact.deduplicatedNumber + 1)
      }
      .getOrElse(value)

    myStateStore.put(updatedContact.userId, updatedContact)

    if (updatedContact.isFresh) {
      Some(value)
    } else None
  }

  override def close(): Unit = {}
}
