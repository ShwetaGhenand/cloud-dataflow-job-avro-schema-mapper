package com.example.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import com.google.cloud.spanner.Struct;

public class CommonUtil {

  private CommonUtil() {

  }

  public static Schema getSchema() {

    Schema productDetails = SchemaBuilder.record("ProductDetails").namespace("com.example.avro")
        .fields().name("product_brand").type().stringType().noDefault().endRecord();

    Schema itemSchema = SchemaBuilder.record("Item").namespace("com.example.avro").fields()
        .requiredString("prodcut_id").name("product_name").type().stringType().noDefault()
        .name("product_item_number").type().longType().noDefault().name("productDetails")
        .type(productDetails).noDefault().endRecord();

    return itemSchema;
  }

  public static byte[] getAvroSchemaBasedRecord(Struct row) throws IOException {

    Schema itemSchema = getSchema();
    GenericRecordBuilder builder = new GenericRecordBuilder(itemSchema);
    List<Schema.Field> fields = itemSchema.getFields();

    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Schema avroType = field.schema();

      switch (avroType.getType()) {
      case STRING:
        switch (row.getColumnType(fieldName).toString()) {
        case "INT64":
          builder.set(field, Long.toString(row.getLong(fieldName)));
          break;
        case "STRING":
          builder.set(field, row.getString(fieldName));
          break;
        default:
          break;
        }
        break;

      case LONG:
        switch (row.getColumnType(fieldName).toString()) {
        case "STRING":
          builder.set(field, Long.valueOf(row.getString(fieldName)).longValue());
          break;
        default:
          break;
        }
        break;

      case RECORD:
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(field.schema());
        List<Schema.Field> subRecordFields = field.schema().getFields();
        for (Schema.Field recordField : subRecordFields) {
          String recordfieldName = recordField.name();
          Schema recordType = recordField.schema();

          switch (recordType.getType()) {
          case STRING:
            recordBuilder.set(recordField, row.getString(recordfieldName));
          default:
            break;
          }
        }
        builder.set(field, recordBuilder.build());
        break;
      default:
        break;
      }

    }
    GenericRecord record = builder.build();
    return avroSerializer(record);

  }

  public static byte[] avroSerializer(GenericRecord record) throws IOException {
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
    ByteArrayOutputStream byteoutputStream = new ByteArrayOutputStream();
    byteoutputStream.reset();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteoutputStream, null);

    writer.write(record, encoder);
    encoder.flush();
    byteoutputStream.flush();

    return byteoutputStream.toByteArray();

  }

}
