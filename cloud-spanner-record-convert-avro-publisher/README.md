#### Convert Spanner Record to Avro




###### Data Pipeline Execution Command
```
mvn compile exec:java -Dexec.mainClass=com.example.StarterPipeline -Dexec.args=" --kafkaURL=localhost:9092 --topicName=avro-topic --instanceName=test-instance-01 --databaseName=test-db-01"
```