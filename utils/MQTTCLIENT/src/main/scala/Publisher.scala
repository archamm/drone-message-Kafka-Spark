import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import Array._
import scala.annotation.tailrec

object Publisher {

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
          val msg = "{\"DroneId\": \"%s\", \"message\": \"%s\"}".format(droneId, weightedSelect("BAD_PARKING_0" -> 33, "BAD_PARKING_1"-> 33, "BAD_PARKING_2"-> 33,
            "CANT_TAKE_ACTION"-> 1).take(1).head)
          val message = new MqttMessage(msg.getBytes("utf-8"))

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