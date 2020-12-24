val jacksonVersion = "2.10.5"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-example",
    organization := "mwrobel.com",
    version := "1.0",
    libraryDependencies ++= Seq(
        "ch.qos.logback"                 % "logback-classic"          % "1.2.3",
        "org.apache.kafka"               %% "kafka-streams-scala"     % "2.2.+",
        "org.apache.kafka"               % "kafka-streams-test-utils" % "2.2.+" % Test,
        "org.scalatest"                  %% "scalatest"               % "3.0.+" % Test,
        "com.typesafe.scala-logging"     %% "scala-logging"           % "3.9.2",
        "com.fasterxml.jackson.module"   %% "jackson-module-scala"    % jacksonVersion,
        "com.fasterxml.jackson.core"     % "jackson-databind"         % jacksonVersion,
        "com.fasterxml.jackson.datatype" % "jackson-datatype-joda"    % jacksonVersion
      ),
    scalaVersion := "2.12.7",
    scalafmtOnCompile := true,
    Test / parallelExecution := false
  )
