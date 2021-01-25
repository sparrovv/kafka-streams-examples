package com.mwrobel.kafkastreams.example_commands

import com.mwrobel.kafkastreams.example_commands.models.Commands
import com.mwrobel.kafkastreams.example_commands.models.Commands.StateChangeCommand

object SpecialDeserializer {
  def deserialiseToTheRightType(command: Array[Byte]): Option[StateChangeCommand] = {
    val rawEvent = new String(command)

    def regex(eventName: String) = {
      val x = s""""eventName":"${eventName}""""
      x.r
    }

    val foo =
      Commands.allCommandsSerdes
        .map {
          case (k, serde) => {
            val r: Boolean = regex(k).findFirstIn(rawEvent).isDefined
            (r, serde)
          }
        }
        .filter { case (k, _) => k }
        .map { case (_, v) => v }
        .toSeq
        .lift(0)

    val x: Option[StateChangeCommand] =
      foo.flatMap(v => Option(v.deserializer().deserialize("", command)))

    x
  }
}
