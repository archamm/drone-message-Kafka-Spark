name := "NYPDCsvToS3"


lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.13.2",
      version := "0.2.0-SNAPSHOT"
    )),
    name := "NYPDCsvToS3",


    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
    ),

    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.11.788",
      "com.amazonaws" % "amazon-kinesis-client" % "1.11.2",
      "org.apache.hadoop" % "hadoop-common" % "3.0.0",
      "javax.activation" % "activation" % "1.1.1",
      "org.apache.hadoop" % "hadoop-client" % "3.0.0",
      "org.apache.hadoop" % "hadoop-aws" % "3.0.0",
      "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar") // this is a workaround for https://github.com/jax-rs/api/issues/571
    )


  )