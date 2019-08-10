package com.example.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
  @Description("kafka URL command line argument.")
  @Default.String("localhost:9092")
  String getKafkaURL();

  void setKafkaURL(String kafkaURL);

  @Description("Topic name command line argument.")
  @Default.String("avro-topic")
  String getTopicName();

  void setTopicName(String topicName);

  @Description("Instance name command line argument.")
  @Default.String("test-instance-01")
  String getInstanceName();

  void setInstanceName(String instanceName);

  @Description("Database Name command line argument.")
  @Default.String("test-db-01")
  String getDatabaseName();

  void setDatabaseName(String databaseName);
}