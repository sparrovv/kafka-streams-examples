package com.mwrobel.kafkastreams.serdes

import java.util

import com.mwrobel.kafkastreams.utils.JsonUtil
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class JsonSerde[T: TypeTag: ClassTag] extends Serde[T] {

  override def deserializer(): Deserializer[T] = new Deserializer[T] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): T = {
      JsonUtil.fromJson[T](data)
    }
  }

  override def serializer(): Serializer[T] = new Serializer[T] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: T): Array[Byte] = {
      if (data != null) {
        JsonUtil.toJson(data).getBytes
      } else null
    }

    override def close(): Unit = {}
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}
