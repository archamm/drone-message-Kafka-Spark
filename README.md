# drone-message-Kafka-Spark

A project where i had to simulate drones sending large quantities of data, and then create an architecture to ETL it.
![technoUsed](https://github.com/archamm/drone-message-Kafka-Spark/blob/master/utils/img/technoUsed.png)

![Architecture](https://github.com/archamm/drone-message-Kafka-Spark/blob/master/utils/img/Architecture.png)



### Requirements

- Java 8
- [Confluent Platform 5.0+](https://www.confluent.io/download/) 
- MQTT Client and Broker (I use [Mosquitto](https://mosquitto.org/download/))
- [Confluent MQTT Connector](https://www.confluent.io/connector/kafka-connect-mqtt/) (a Kafka Connect based connector to send and receive MQTT messages) - Very easy installation via Confluent Hub, just one command:

                confluent-hub install confluentinc/kafka-connect-mqtt:1.2.3


### Starting backend services
- Start mosquitto 

    on macOS


                 brew services start mosquitto  
    on linux

                 sudo service  mosquitto start  

- Start Zookeeper / kafka / Connect

      Go to ${confluent installation folder}/bin

                ./confluent start connect

- Add kafka mqtt connector
                
                curl -s -X POST -H 'Content-Type: application/json' http://localhost:8083/connectors -d '{
                    "name" : "mqtt-drones-messages",
                    "config" : {
                    "connector.class" : "io.confluent.connect.mqtt.MqttSourceConnector",
                    "tasks.max" : "1",
                    "mqtt.server.uri" : "tcp://127.0.0.1:1883",
                    "mqtt.topics" : "/drones/messages",
                    "kafka.topic" : "mqtt.drones-messages",
                    "confluent.topic.bootstrap.servers": "localhost:9092",
                    "confluent.topic.replication.factor": "1",
                    "confluent.license":""
                    }
                }'

    You can test that your installation is working by typing 
                  http://localhost:8083/connectors/mqtt-drones-messages/status
    in your browser, if there is an exception, something went wrong.

- Add Kafka mqtt.drones-messages topic

                ./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mqtt.drones-messages

### Link and launch the drone message consumer


- Link it to the S3

- Create an S3 bucket.

- Write your bucket name, your aws authentification key and password in kafka-consumer/src/main/scala/DroneConsumer.scala


- Launch the mqtt drones messages generator in  utils/MQTTCLIENT/src/main/scala/Publisher.scala

- Launch the DroneConsumer in kafka-consumer/src/main/scala/DroneConsumer.scala

The generated drones messages should all be classified in the csv hosted on the S3

### Launch the alert message consumer
  
- Launch the mqtt drones messages generator in  utils/MQTTCLIENT/src/main/scala/Publisher.scala

- Write your aws authentification key and password in Kafka-Alerts-Consumer/src/main/scala/AlertsConsumer.scala

- Launch the  in Kafka-Alerts-Consumer/src/main/scala/AlertsConsumer.scala

- Start react app: 

                $ cd alert-react-app/alert-react-app
                $ yarn start


