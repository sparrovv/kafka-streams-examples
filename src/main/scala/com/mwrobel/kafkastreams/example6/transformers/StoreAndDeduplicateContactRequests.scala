package com.mwrobel.kafkastreams.example6.transformers

import com.mwrobel.kafkastreams.example6.ContactRequestsStore
import com.mwrobel.kafkastreams.example6.models.ContactRequest
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext

class StoreAndDeduplicateContactRequests extends ValueTransformer[ContactRequest, ContactRequest] {
  var myStateStore: ContactRequestsStore.MySuperStore = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(ContactRequestsStore.name).asInstanceOf[ContactRequestsStore.MySuperStore]
  }

  override def transform(value: ContactRequest): ContactRequest = {
    val updatedContact = Option(myStateStore.get(value.userId))
      .map { contact =>
        contact.copy(deduplicatedNumber = contact.deduplicatedNumber + 1)
      }
      .getOrElse(value)

    myStateStore.put(updatedContact.userId, updatedContact)

    if (updatedContact.isFresh) {
      value
    } else null
  }

  override def close(): Unit = {}
}
