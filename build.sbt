//val jacksonVersion = "2.9.1"

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
      "com.typesafe.scala-logging"   %%    "scala-logging"     % "3.9.2",

      "com.fasterxml.jackson.module"   %% "jackson-module-scala" % "2.9.1",
      "com.fasterxml.jackson.core"     % "jackson-databind"      % "2.9.1",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.9.1"
    ),
    scalaVersion := "2.12.7",
    scalafmtOnCompile := true
  )
