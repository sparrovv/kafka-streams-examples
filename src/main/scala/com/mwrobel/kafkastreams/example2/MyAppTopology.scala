package com.mwrobel.kafkastreams.example2

import com.mwrobel.kafkastreams.Topics
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.KStream

object MyAppTopology {
  import Serdes._

  def buildTopology()(implicit builder: StreamsBuilder): Unit = {

    val textLines: KStream[String, String] = builder.
      stream[String, String](Topics.inputTopic)

    val wordCounts  = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .filter( (_, word) => word.length() > 5)

    wordCounts.to(Topics.outputTopic)
  }
}
