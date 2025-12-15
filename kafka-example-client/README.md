# Kafka Client (Producer & Consumer)

This module provides a Java application that can act as either a Kafka producer (sending random JSON messages) or a consumer, based on command-line arguments.

## Build

```
mvn -f kafka-example-client/pom.xml clean package
```

## Run

```
# Run the shaded jar produced in target/
java -jar target/kafka-random-producer-1.0.0.jar [options]
```

## Options

- `--brokers <brokers>`: Kafka bootstrap servers 
- `--topic <topic>`: Kafka topic name 
- `--username <username>`: SASL username 
- `--password <password>`: SASL password 
- `--process <consumer|producer>`: Run as consumer or producer 
- `--help`: Show usage and exit

## Examples


Run as a producer (send random messages):

```
java -jar kafka-example-client/target/kafka-example-client-1.0.0.jar \
	--process producer \
	--brokers <BROKER_URL> \
	--topic <TOPIC_NAME> \
	--username <USERNAME> \
	--password <PASSWORD>
```

Run as a consumer:

```
java -jar kafka-example-client/target/kafka-example-client-1.0.0.jar \
	--process consumer \
	--brokers <BROKER_URL> \
	--topic <TOPIC_NAME> \
	--username <USERNAME> \
	--password <PASSWORD>
```

## Notes

- The default credentials and brokers are set for Confluent Cloud. Override them for local or other clusters.
- The producer sends random JSON messages. The consumer reads messages from the specified topic.
- Press Ctrl+C to stop; the client closes gracefully.
