
val sparkVersion = "2.2.0"
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version := "0.2.0-SNAPSHOT"
    )),
    name := "spark-analyse",


    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
    ),

      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-mllib" % sparkVersion,
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "org.apache.hadoop" % "hadoop-common" % "3.0.0",
        "org.apache.hadoop" % "hadoop-client" % "3.0.0",
        "org.apache.hadoop" % "hadoop-aws" % "3.0.0",
        "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar") // this is a workaround for https://github.com/jax-rs/api/issues/571
      )


  )