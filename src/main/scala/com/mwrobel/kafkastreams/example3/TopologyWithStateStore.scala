package com.mwrobel.kafkastreams.example3

import com.mwrobel.kafkastreams.Topics
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}


case class WordDetails(count:Int)

object MyStore {
  val name = "foo"

  val keySerde = Serdes.String
  val valSerde = Serdes.Integer

  type MySuperStore = KeyValueStore[String, Int]
}

class PersistTransformer extends ValueTransformer[String, String]{
  var myStateStore: MyStore.MySuperStore = _

  override def init(context: ProcessorContext): Unit = {
    myStateStore = context.getStateStore(MyStore.name).asInstanceOf[MyStore.MySuperStore]
  }

  override def transform(value: String): String = {
    val newRecord = Option(myStateStore.get(value))
      .map(r => r + 1)
      .getOrElse(1)

    myStateStore.put(value, newRecord)

    value
  }

  override def close(): Unit = {}
}

object TopologyWithStateStore {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {
    // defining a state store
    val storeSupplier = Stores.persistentKeyValueStore(MyStore.name)
    val storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, MyStore.keySerde, MyStore.valSerde)
    builder.addStateStore(storeBuilder)

    // creating a transfomer that's a part of Processor API
    val transformSupplier = new ValueTransformerSupplier[String, String] {
      override def get(): ValueTransformer[String, String] = new PersistTransformer()
    }

    builder.stream[String, String](Topics.inputTopic)
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .filter( (_, word) => word.length() > 5)
      .transformValues(transformSupplier, MyStore.name)
  }
}
