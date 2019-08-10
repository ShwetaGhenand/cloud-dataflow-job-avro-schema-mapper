
## Avro Record Subscription

- Read Avro Record from kafka.
- Deserlialze Avro Record.



###### Data Pipeline Execution Command
```
mvn compile exec:java -Dexec.mainClass=com.example.StarterPipeline -Dexec.args=" --kafkaURL=localhost:9092 --topicName=avro-topic"
```
