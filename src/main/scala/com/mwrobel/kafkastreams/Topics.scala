package com.mwrobel.kafkastreams

//kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic TextLinesTopic
//kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic LongerWords
object Topics {
  val inputTopic  = "TextLinesTopic"
  val outputTopic = "LongerWords"
}
