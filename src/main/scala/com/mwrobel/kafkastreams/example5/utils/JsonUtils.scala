package com.mwrobel.kafkastreams.example5.utils

import java.text.SimpleDateFormat
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.util.control.NonFatal

/*
 * This object provides utility methods to work with JSON messages across SB
 */
object JsonUtil {
  // used by extractJson
  private lazy val extractJsonMapper = new ObjectMapper

  private val mapper = new ObjectMapper() with ScalaObjectMapper

  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JodaModule())
  mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES)
  mapper.setSerializationInclusion(NON_NULL)

  def toJson[T](value: T): String = mapper.writeValueAsString(value)

  def toJsonBytes[T](value: T): Array[Byte] = mapper.writeValueAsBytes(value)

  def fromJson[T](json: Array[Byte])(implicit m: Manifest[T]): T = mapper.readValue[T](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = mapper.readValue[T](json)

  def readNode[T](jsonNode: JsonNode)(implicit m: Manifest[T]): T = mapper.treeToValue(jsonNode)

  /**
    * Warning: this method is convenient, but also slow and dangerous.
    *
    * - Slow because it serializes a class to bytes and then deserializes it back to the class you want. It
    *   might slow things down considerably if you need to map over lots of objects. If speed is needed,
    *   consider hand-crafting your mapping function to see if that speeds things up.
    * - Dangerous because types are not checked when deserialising. This method might throw JsonParseException
    *   or JsonMappingException if the types are not correct.
    * @param a the object you want to map
    * @tparam A the original type of the object
    * @tparam B the type that you want to map to
    * @return the mapped object
    */
  def map[A, B](a: A)(implicit m: Manifest[B]): B = fromJson[B](toJsonBytes(a))
}
