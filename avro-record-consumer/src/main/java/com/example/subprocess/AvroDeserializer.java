package com.example.subprocess;

import java.io.IOException;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.util.CommonUtil;

public class AvroDeserializer extends DoFn<KafkaRecord<String, byte[]>, String> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializer.class);

  /**
   * 
   */
  private static final long serialVersionUID = -1128677813112165758L;

  @ProcessElement
  public void processElement(ProcessContext pc) throws IllegalStateException, IOException {
    LOG.debug("avro desrializer transform is started");

    Object object = CommonUtil.deserialize(pc.element().getKV().getValue());
    LOG.info("object::" + object);

    LOG.debug("avro desrializer transform is finished");

  }

}
