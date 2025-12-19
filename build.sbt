ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BatchFileProducer",

    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play-json" % "2.10.0-RC7",
      "org.apache.kafka" % "kafka-clients" % "3.8.0",
      "org.slf4j" % "slf4j-simple" % "2.0.9"
    )
  )
