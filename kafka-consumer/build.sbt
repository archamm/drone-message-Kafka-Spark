
import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.13.2",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "kafka-consumer",
    version := "0.1",

    resolvers ++= Seq (
      Opts.resolver.mavenLocalFile,
      "Confluent" at "http://packages.confluent.io/maven"
    ),

    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.11.788",
      "com.typesafe" % "config" % "1.3.2",
      "org.apache.kafka" % "kafka-clients" % "2.4.0",
      "org.apache.kafka" % "connect-json" % "2.4.0",
      "org.apache.kafka" % "connect-runtime" % "2.4.0",
      "org.apache.kafka" % "kafka-streams" % "2.4.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "2.4.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.0",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
      "org.apache.hadoop" % "hadoop-common" % "2.6.0",
      "org.apache.commons" % "commons-io" % "1.3.2",
      "org.apache.kafka" % "connect-runtime" % "2.1.0",
      "io.confluent" % "kafka-json-serializer" % "5.0.1",
      "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar") // this is a workaround for https://github.com/jax-rs/api/issues/571
    )


  )
