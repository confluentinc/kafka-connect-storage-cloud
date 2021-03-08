package io.confluent.connect.s3.format.csv;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Pattern;

public class CSVConverter implements Converter, HeaderConverter {

    private static final ConfigDef CONFIG_DEF = CSVConverterConfig.configDef();
    private CSVConverterConfig config;
    private String fieldSeparator;
    private Schema lastSchema;

    public CSVConverter() {}

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void configure(Map<String, ?> configs) {
        this.config = new CSVConverterConfig(configs);
        this.fieldSeparator = config.getString("csv.field.sep");
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(configs);
    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (value != null && schema == null) {
            throw new DataException("CSVConverter requires schema to be set");
        } else {
            // TODO: allow to specify charset if needed.
            return toCSVData(schema, value == null ? schema.defaultValue() : value).getBytes(Charset.defaultCharset());
        }
    }

    public byte[] getHeader() {
        if (lastSchema == null) {
            return null;
        }
        // TODO: allow to specify charset if needed in future.
        return schemaToHeader("", lastSchema).getBytes(Charset.defaultCharset());
    }

    private String schemaToHeader(String prefix, Schema schema) {
        switch (schema.type()) {
            case STRUCT:
                StringBuilder builder = new StringBuilder();
                for (Field f: schema.fields()) {
                    if (builder.length() > 0) {
                        builder.append(this.fieldSeparator);
                    }
                    builder.append(schemaToHeader(toSnakeCase(f.name()), f.schema()));
                }
            default:
                if (StringUtils.isBlank(prefix)) {
                    return toSnakeCase(schema.name());
                } else {
                    return prefix + "_" + schema.name();
                }
        }
    }

    private final Pattern CASE_CHANGE_PATTERN = Pattern.compile("([a-z])(A-Z)");

    private String toSnakeCase(String name) {
        return CASE_CHANGE_PATTERN.matcher(name).replaceAll("$1_$2").toLowerCase();
    }

    private String toCSVData(Schema schema, Object value) {
        this.lastSchema = schema;
        if (schema == null) {
            return "";
        }
        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct)value;
                if (!struct.schema().equals(schema)) {
                    throw new DataException("Mismatching schema.");
                }
                StringBuilder buf = new StringBuilder();
                for (Field f: struct.schema().fields()) {
                    if (buf.length() > 0) {
                        buf.append(this.fieldSeparator);
                    }
                    buf.append(toCSVData(f.schema(), struct.get(f)));
                }
                return buf.toString().replaceAll("\"", "\"\"");
            case MAP:
                throw new DataException("Map is not supported");
            default:
                return "\"" + value + "\"";
        }

    }

    public SchemaAndValue toConnectData(String topic, byte[] value) {
        throw new UnsupportedOperationException("Converting bytes to connect data is not yet supported. This is converter only for Sink connector");
    }

    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return this.fromConnectData(topic, schema, value);
    }

    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return this.toConnectData(topic, value);
    }

    public void close() {
    }
}
