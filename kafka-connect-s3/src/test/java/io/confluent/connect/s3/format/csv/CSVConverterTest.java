package io.confluent.connect.s3.format.csv;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;


import java.time.ZoneId;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class CSVConverterTest {

  @Test
  public void headerAndDataForSimpleSchema() {
    Schema simpleSchema = getSimpleSchema();
    Date testDate = new Date(333333333L);
    Date testTime = new Date(111111111L);
    Struct value = new Struct(simpleSchema)
            .put("textField", "test\"Value")
            .put("numericField", 123L)
            .put("dateField", testDate)
            .put("boolField", false)
            .put("timeField", testTime);
    CsvConverter converter = new CsvConverter();
    assertEquals("\"test\"\"Value\",\"false\",,\"123\",\""
                    +testDate.toInstant()+"\",\""
                    +testTime.toInstant()+"\"",
            new String(converter.fromConnectData("topic", simpleSchema, value)));
    assertEquals("\"text_field\",\"bool_field\",\"null_field\","
                   + "\"numeric_field\",\"date_field\",\"time_field\"",
            new String(converter.getHeader()));
  }

  @Test
  public void headerAndDataForNestedSchema() {
    Schema nestedSchema = getNestedSchema();
    Date testDate = new Date(333333333L);
    Date testTime = new Date(111111111L);
    Struct value = new Struct(nestedSchema.field("nested").schema())
            .put("textField", "test\"Value")
            .put("numericField", 123L)
            .put("dateField", testDate)
            .put("timeField", testTime);
    Struct nestedValue = new Struct(nestedSchema);
    nestedValue.put("nested",value);
    nestedValue.put("textField", "randomValue");
    CsvConverter converter = new CsvConverter();
    assertEquals("\"randomValue\",,,,,,,\"test\"\"Value\",,,\"123\",\""
                    +testDate.toInstant()+"\",\""
                    +testTime.toInstant()+"\"",
            new String(converter.fromConnectData("topic", nestedSchema, nestedValue)));
    assertEquals("\"text_field\","
                    +"\"missing_field_text_field\",\"missing_field_bool_field\","
                    +"\"missing_field_null_field\",\"missing_field_numeric_field\""
                    +",\"missing_field_date_field\","
                    +"\"missing_field_time_field\",\"nested_text_field\",\"nested_bool_field\""
                    +",\"nested_null_field\","
                    +"\"nested_numeric_field\",\"nested_date_field\",\"nested_time_field\"",
            new String(converter.getHeader()));
  }

  @Test
  public void fixedRowHeader() {
    Schema nestedSchema = getNestedSchema();
    Date testDate = new Date(333333333L);
    Date testTime = new Date(111111111L);
    Struct value = new Struct(nestedSchema.field("nested").schema())
            .put("textField", "test\"Value")
            .put("numericField", 123L)
            .put("dateField", testDate)
            .put("timeField", testTime)
            .put("boolField", true);
    Struct nestedValue = new Struct(nestedSchema);
    nestedValue.put("nested",value);
    nestedValue.put("textField", "randomValue");
    CsvConverter converter = new CsvConverter();
    Map<String, Object> config = new HashMap<String, Object>();
    List<String> headers = Lists.newArrayList("nested.dateField","textField",
            "nested.boolField","nothing");
    config.put("csv.compat", true);
    config.put("csv.fields.list", headers);
    converter.configure(config);
    assertEquals("\""+testDate.toInstant().atZone(ZoneId.systemDefault()
            ).format(CsvConverter.COMPAT_FORMAT)+"\",\"randomValue\",\"1\",",
            new String(converter.fromConnectData("topic", nestedSchema, nestedValue)));
    assertEquals("\"nested_date_field\",\"text_field\",\"nested_bool_field\",\"nothing\"",
            new String(converter.getHeader()));
  }

  private Schema getSimpleSchema() {
    return SchemaBuilder
            .struct()
            .name("TestSchema")
            .parameter("namespace", "com.test")
            .field("textField", Schema.STRING_SCHEMA)
            .field("boolField", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("nullField", SchemaBuilder.string().optional())
            .field("numericField", Schema.INT64_SCHEMA)
            .field("dateField", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("timeField", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .build();
  }

  private Schema getNestedSchema() {
    return SchemaBuilder
            .struct()
            .name("NestedSchema")
            .parameter("namespace", "com.test")
            .field("textField", Schema.STRING_SCHEMA)
            .field("missingField", getSimpleSchema()).optional()
            .field("nested", getSimpleSchema())
            .build();
  }
}
