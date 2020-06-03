import java.io.{BufferedOutputStream, File, FileOutputStream, FileReader, FileWriter}
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
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesisfirehose.{AmazonKinesisFirehose, AmazonKinesisFirehoseClient}
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest
import java.nio.ByteBuffer

import com.amazonaws.services.kinesisfirehose.model.Record

import scala.annotation.tailrec

object DroneConsumer {

  case class DroneMessage(DroneId: Int, violation: ViolationObject, date: String, latitude:Double, longitude: Double)
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
        writeInS3(rec.timestamp(), rec.value().toString)
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


  def writeInS3(key: Long, message: String) :Unit = {


    //file to upload
    /* These Keys would be available to you in  "Security Credentials" of
        your Amazon S3 account */
    val AWS_ACCESS_KEY = "AKIAS7AOU2S4KEKI3Q7K"
    val AWS_SECRET_KEY = "nw9S4aHLcOcIMRvo2MWrz6+/A/wKGL9YowLi9Tpt"
    val provider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY)
    )
    val firehoseClient = AmazonKinesisFirehoseClient.builder.withRegion(Regions.EU_WEST_3)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY))).build

    sendToFireHose(firehoseClient, formatForFireHose(message))
  }

  def sendToFireHose(firehose: AmazonKinesisFirehose, message: String): Unit ={
    val fireHoseDeliveryStreamName = "drones-messages-flux"
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setDeliveryStreamName(fireHoseDeliveryStreamName)

    val record = new Record()
      .withData(ByteBuffer.wrap(message.getBytes))
    putRecordRequest.setRecord(record)
    val putRecordResult = firehose.putRecord(putRecordRequest)
    println(putRecordResult.toString)
  }

  def writeToLocalFiles(message: MessageObject):Unit = {
    try{

      val fw = new FileWriter("drones-messages.csv", true)
      val fwVio = new FileWriter("drones-violations-messages.csv", true)
      try {
        fw.write("\n" + message.regularMsg )
        if (message.violationMessage != null)
          fwVio.write("\n" + message.violationMessage)
      }
      finally {
        fw.close()
        fwVio.close()
      }}
    catch {
      case e : Exception=>  println("Error when parsing or writing message", e)
    }
  }

  def writeMessageInS3(amazonS3Client: AmazonS3, fileName: String, message: String, bucketName: String): Boolean = {
    val file = new File(fileName)
    val o = amazonS3Client.getObject(bucketName, fileName)
    val s3is = o.getObjectContent
    writeBytes(s3is, file)
    s3is.close()
    val fos = new FileOutputStream(file, true)
    fos.write(("\n" + message).getBytes)
    amazonS3Client.putObject(bucketName, fileName, file)
    FileUtils.deleteQuietly(file)
  }

  def formatForFireHose(message: String): String ={
    val formattedMessage :String = message.substring(message.indexOf("{"))
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val droneMessage = objectMapper.readValue(formattedMessage, classOf[DroneMessage])
    if (droneMessage.violation != null) {
      droneMessage.DroneId.toString + ";" + droneMessage.violation.violationId + ";" + droneMessage.violation.imageId +
        ";" + droneMessage.violation.violationCode + ";" +
        droneMessage.date + ";" + droneMessage.latitude + ";" + droneMessage.longitude + "\n"
    }
    else {
      droneMessage.DroneId.toString + ";" + "" + ";" + "" +
        ";" + "" + ";" + droneMessage.date + ";" + droneMessage.latitude + ";" + droneMessage.longitude  + "\n"
    }

    }
  def formatDroneMessage(message:String): MessageObject = {
    val formattedMessage :String = message.substring(message.indexOf("{"))
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val droneMessage = objectMapper.readValue(formattedMessage, classOf[DroneMessage])
    if (droneMessage.violation != null) {
       MessageObject(droneMessage.DroneId.toString + ";" + droneMessage.violation.violationId + ";" +
         droneMessage.date + ";" + droneMessage.latitude + ";" + droneMessage.longitude,
         droneMessage.violation.violationId + ";" + droneMessage.violation.imageId + ";" + droneMessage.violation.violationCode)
    }
    else {
        MessageObject(droneMessage.DroneId.toString + ";" + "" + ";" + droneMessage.date + ";" + droneMessage.latitude + ";" + droneMessage.longitude,
          null)
      }

  }
  def writeBytes( data : S3ObjectInputStream, file : File ): Unit = {
    val target = new BufferedOutputStream( new FileOutputStream(file) )
    try data.readAllBytes().foreach( target.write(_) ) finally target.close()
  }



  @tailrec def forever[A](body: => A): Nothing = {
    body
    forever(body)
    }

}