package com.example.util;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

public class CommonUtil {

  private CommonUtil() {

  }

  public static Schema getSchema() {
    Schema productDetails = SchemaBuilder.record("ProductDetails").namespace("com.example.avro")
        .fields().name("product_brand").type().stringType().noDefault().endRecord();

    Schema item = SchemaBuilder.record("Item").namespace("com.example.avro").fields()
        .requiredString("prodcut_id").name("product_name").type().stringType().noDefault()
        .name("product_item_number").type().longType().noDefault().name("productDetails")
        .type(productDetails).noDefault().endRecord();
    return item;
  }

  public static Object deserialize(byte[] record) throws IOException, IllegalStateException {
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(getSchema());
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record, null);

    return reader.read(null, decoder);
  }
}
