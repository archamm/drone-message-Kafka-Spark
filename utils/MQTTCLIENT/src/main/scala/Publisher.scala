import java.util.UUID

import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import Array._
import scala.annotation.tailrec
import scala.util.Random
import java.time.LocalDateTime
import java.time._

object Publisher {
  def buildMessage(droneId:Int): String = {
    if (isThereAViolation) {
      println("VIOLATION")
      "{\"DroneId\": \"%s\", \"violation\": %s, \"date\": \"%s\", \"location\": \"%s\"}".format(droneId, buildViolationObject, getRandomDate, getRandomLocation.toString())
    }
    else
      "{\"DroneId\": \"%s\", \"date\": \"%s\", \"location\": \"%s\"}".format(droneId, getRandomDate, getRandomLocation.toString())
  }

  def buildViolationObject: String = {
    "{\"violationId\": \"%s\", \"imageId\": \"%s\", \"violationCode\": \"%s\"}".format(UUID.randomUUID.toString, UUID.randomUUID.toString, getRandomViolation)
  }

  def isThereAViolation: Boolean = {
    Random.between(0, 10) < 2
  }

  def getRandomViolation: String = {
    weightedSelect("BAD_PARKING_0" -> 33, "BAD_PARKING_1"-> 33, "BAD_PARKING_2"-> 33,
      "REQUIRE_HUMAN"-> 1).take(1).head
  }

  def getRandomLocation: (Double, Double) = {
    val latitude = Random.between(40.6808, 40.8808)
    val longitude = Random.between(-73.0772, -72.8772)
    (latitude, longitude)
  }

  def getRandomDate: String = {
    val start = LocalDateTime.of(2020, 1, 20,11,48,34)
    val end   = LocalDateTime.of(2020, 5, 20, 14,32,55)
    LocalDateTime.ofEpochSecond(Random.between(start.toEpochSecond(ZoneOffset.UTC),
      end.toEpochSecond(ZoneOffset.UTC)), 0, ZoneOffset.UTC).toString
  }


  @tailrec def forever[A](body: => A): Nothing = {
    body
    forever(body)
  }

  def weightedSelect[T](input :(T, Int)*): List[T] = {
    val items  :Seq[T]    = input.flatMap{x => Seq.fill(x._2)(x._1)}
    def output :List[T] = util.Random.shuffle(items).toList
    output
  }

  def main(args: Array[String]) {
    val brokerUrl = "tcp://localhost:1883"
    val topic = "/drones/messages"
    val droneList = range(1, 200)



    // Creating new persistence for mqtt client
    val persistence = new MqttDefaultFilePersistence("/tmp")
    val client: MqttClient = new MqttClient(brokerUrl, "Drone1", persistence)

    try {
      // mqtt client with specific url and client id

      client.connect()

      val msgTopic = client.getTopic(topic)

      forever {
        droneList.foreach(droneId => {
          val message = new MqttMessage(buildMessage(droneId).getBytes("utf-8"))
          msgTopic.publish(message)
          println("Publishing Data, Topic : %s, Message : %s".format(msgTopic.getName, message))
          Thread.sleep(1000)

        })
      }
    }

    catch {
      case e: MqttException => println("Exception Caught: " + e)
    }

    finally {
      client.disconnect()
    }
  }
}