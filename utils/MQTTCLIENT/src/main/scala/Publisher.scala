import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence

import Array._
import scala.util.Random

object Publisher {
  case class WeightedItem[T](item: T, weight: Double)


  def weightedSelection[T](
                            items: Seq[WeightedItem[T]],
                            numSelections: Int,
                            r: Random
                          ): Seq[T] = {

      val totalWeight = items.map(_.weight).sum

      def pick_one: T = {
        var rnd = r.nextDouble * totalWeight
        for (i <- items) {
          if (i.weight > rnd) {
            return i.item
          }
          rnd = rnd - i.weight
        }
        // the above should always return something, but compiler doesn't
        // realise that, hence this:
        return items.head.item
      }

      for (i <- 1 to numSelections) yield pick_one
  }


  def main(args: Array[String]) {
    val brokerUrl = "tcp://localhost:1883"
    val topic = "/drones/messages"
    val droneList = range(1, 200)
    val items = Seq(WeightedItem("BAD_PARKING_0", 33d/100), WeightedItem("BAD_PARKING_1", 33d/100),
      WeightedItem("BAD_PARKING_2", 33d/100 ), WeightedItem("CANT_TAKE_ACTION", 1d/100 ) )

    println(weightedSelection(items, 1, Random))
    var client: MqttClient = null

    // Creating new persistence for mqtt client
    val persistence = new MqttDefaultFilePersistence("/tmp")

    try {
      // mqtt client with specific url and client id
      client = new MqttClient(brokerUrl, "Drone1", persistence)

      client.connect()

      val msgTopic = client.getTopic(topic)

      while (true) {
        droneList.foreach(droneId => {
          val msg = "{DroneId: %s, message: %s}".format(droneId, weightedSelection(items, 1, Random).head)
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