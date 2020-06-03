import java.io.FileReader
import java.time.Duration
import java.util.{Collections, Properties}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.SendMessageRequest

import scala.annotation.tailrec

object AlertsConsumer {

  case class DroneMessage(DroneId: Int, violation: ViolationObject, date: String, latitude:Double, longitude: Double)
  case class ViolationObject(violationId: String, imageId: String, violationCode: Int)
  case class MessageObject(regularMsg: String, violationMessage: String)

  def main(args: Array[String]): Unit = {
    val topicName = "mqtt.drones-messages"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    val consumer = new KafkaConsumer[String, JsonNode](props)
    //val MAPPER = new ObjectMapper
    consumer.subscribe(Collections.singletonList(topicName))
    forever {
      println("Polling")
      val records = consumer.poll(Duration.ofMillis(500))
      records.forEach(rec => {
        if (isRecordCode100(rec.value().toString)) {
          println(s"ERROR CODE MESSAGE RECEIVED => message: ${rec.value().toString}")
          sendAlertToAWSSQS(rec.timestamp(), rec.value().toString)
        }
      })
    }
    consumer.close()

  }

  def buildProperties(configFileName: String): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "scala_example_group")
    properties.load(new FileReader(configFileName))
    properties
  }


  def sendAlertToAWSSQS(key: Long, message: String) :Unit = {

    /* These Keys would be available to you in  "Security Credentials" of
        your Amazon S3 account */
    val AWS_ACCESS_KEY = "AKIAS7AOU2S4KEKI3Q7K"
    val AWS_SECRET_KEY = "nw9S4aHLcOcIMRvo2MWrz6+/A/wKGL9YowLi9Tpt"
    val provider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY)
    )


    val sqs = AmazonSQSClientBuilder.standard()
    .withCredentials(provider)
    .withRegion("eu-west-3")
      .build()

    val QUEUE_NAME = "drones-code-100-messages.fifo"
    val formattedMessage :String = message.substring(message.indexOf("{"))

    val queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl
    val sendMessageStandardQueue = new SendMessageRequest()
      .withQueueUrl(queue_url)
      .withMessageGroupId(key.toString)
      .withMessageDeduplicationId(key.toString)
      .withMessageBody(formattedMessage)
    sqs.sendMessage(sendMessageStandardQueue)
  }


  def isRecordCode100(message:String): Boolean = {
    val formattedMessage: String = message.substring(message.indexOf("{"))
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val droneMessage = objectMapper.readValue(formattedMessage, classOf[DroneMessage])
    droneMessage.violation != null && droneMessage.violation.violationCode == 100
  }




  @tailrec def forever[A](body: => A): Nothing = {
    body
    forever(body)
    }

}