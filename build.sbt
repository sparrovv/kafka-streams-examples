lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-example",
    organization := "mwrobel.com",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka-streams-scala"        % "2.2.+",
      "org.apache.kafka"  % "kafka-streams-test-utils"   % "2.2.+" % Test,
      "org.scalatest"    %% "scalatest"                  % "3.0.+" % Test,
      "ch.qos.logback"   % "logback-classic"             % "1.2.3",
      "com.typesafe.scala-logging" %%    "scala-logging" % "3.9.2"
),
    scalaVersion := "2.12.7"
  )
