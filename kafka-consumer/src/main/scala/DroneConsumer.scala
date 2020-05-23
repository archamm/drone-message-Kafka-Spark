import java.io.{BufferedOutputStream, File, FileOutputStream, FileReader}
import java.time.Duration
import java.util.{Collections, Properties}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.S3ObjectInputStream
import org.apache.commons.io.FileUtils

import scala.annotation.tailrec

object DroneConsumer {

  case class DroneMessage(DroneId: Int, violation: ViolationObject, date: String, location: String)
  case class ViolationObject(violationId: String, imageId: String, violationCode: String)
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
        println(s"message: ${formatDroneMessage(rec.value().toString)}")
        writeInS3(rec.timestamp(), formatDroneMessage(rec.value().toString))
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


  def writeInS3(key: Long, messageObject: MessageObject) :Unit = {

    val bucketName = "drones-messages"          // specifying bucket name

    //file to upload
    /* These Keys would be available to you in  "Security Credentials" of
        your Amazon S3 account */
    val AWS_ACCESS_KEY = "AKIAS7AOU2S4LWDP4VVB"
    val AWS_SECRET_KEY = "d4jC/2g6McJaqz+XUaxbY7YXfWrbIkn3v6PooAtO"
    val provider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY)
    )

    val amazonS3Client = AmazonS3ClientBuilder
      .standard
      .withCredentials(provider)
      .withRegion("eu-west-3")
      .build

    writeMessageInS3(amazonS3Client, "drones-messages.csv", messageObject.regularMsg, bucketName)
    if (messageObject.violationMessage != null){
      writeMessageInS3(amazonS3Client, "drones-violations-messages.csv", messageObject.violationMessage, bucketName)
    }
  }

  def writeMessageInS3(amazonS3Client: AmazonS3, fileName: String, message: String, bucketName: String): Boolean = {
    val file = new File(fileName)
    val o = amazonS3Client.getObject(bucketName, fileName)
    val s3is = o.getObjectContent
    writeBytes(s3is, file)
    s3is.close()
    val fos = new FileOutputStream(file, true)
    fos.write(message.getBytes)
    fos.write("\n".getBytes)
    amazonS3Client.putObject(bucketName, fileName, file)
    FileUtils.deleteQuietly(file)
  }

  def formatDroneMessage(message:String): MessageObject = {
    val formattedMessage :String = message.substring(message.indexOf("{"))
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val droneMessage = objectMapper.readValue(formattedMessage, classOf[DroneMessage])
    if (droneMessage.violation != null) {
       MessageObject(droneMessage.DroneId.toString + " ; " + droneMessage.violation.violationId + " ; " + droneMessage.date + " ; " + droneMessage.location,
         droneMessage.violation.violationId + " ; " + droneMessage.violation.imageId + " ; " + droneMessage.violation.violationCode)
    }
    else {
        MessageObject(droneMessage.DroneId.toString + " ; " + "" + " ; " + droneMessage.date + " ; " + droneMessage.location,
          null)
      }

  }
  def writeBytes( data : S3ObjectInputStream, file : File ): Unit = {
    val target = new BufferedOutputStream( new FileOutputStream(file))
    try data.readAllBytes().foreach( target.write(_) ) finally target.close()
  }


  @tailrec def forever[A](body: => A): Nothing = {
    body
    forever(body)
    }

}