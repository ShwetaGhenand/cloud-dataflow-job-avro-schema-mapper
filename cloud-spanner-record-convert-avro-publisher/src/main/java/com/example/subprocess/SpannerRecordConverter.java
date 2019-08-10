package com.example.subprocess;

import java.io.IOException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.util.CommonUtil;
import com.google.cloud.spanner.Struct;

public class SpannerRecordConverter extends DoFn<Struct, KV<String, byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerRecordConverter.class);
  /**
   * 
   */
  private static final long serialVersionUID = 9145437740403661496L;

  @ProcessElement
  public void processElement(ProcessContext pc) throws IOException {
    LOG.debug("spanner record converter pardo is started");
    
    byte[] avroRecord = CommonUtil.getAvroSchemaBasedRecord(pc.element());
    pc.output(KV.of("item", avroRecord));
    
    LOG.debug("spanner record converter pardo is finished");
  }
}
