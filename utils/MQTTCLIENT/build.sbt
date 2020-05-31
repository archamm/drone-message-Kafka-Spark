name := "MQTTCLIENT"

version := "0.1"

scalaVersion := "2.13.2"

libraryDependencies += "org.eclipse.paho" % "mqtt-client" % "0.4.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.0"

resolvers += "MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/"
