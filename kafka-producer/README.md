# Kafka Random Producer

Small Java application that continuously sends random JSON messages to a Kafka topic.

Build
```
mvn -f kafka-producer/pom.xml clean package
```

Run
```
# Run the shaded jar produced in target/
java -jar kafka-producer/target/kafka-random-producer-1.0.0.jar --brokers localhost:9092 --topic test-topic --rate 10
```

Options
- `--brokers`: Kafka bootstrap servers (default `localhost:9092`)
- `--topic`: Kafka topic to produce to (default `test-topic`)
- `--rate`: messages per second (default `10`)

Stop
- Ctrl+C to stop; producer closes gracefully.
