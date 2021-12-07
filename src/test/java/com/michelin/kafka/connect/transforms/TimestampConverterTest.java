package com.michelin.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class TimestampConverterTest {
    private final TimestampMicrosConverter.Value<SourceRecord> smt = new TimestampMicrosConverter.Value<>();

    @Test
    public void shouldConvertTimestampMicroSecondsWithSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put("field", "timestamp");
        properties.put("target.type", "unix");

        smt.configure(properties);

        Schema schemaWithTimestampMicros = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .field("timestamp", SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
                .optional()
                .build();

        Instant now = Instant.now();

        Struct recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
                .put("id", (byte) 1)
                .put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        SourceRecord record = new SourceRecord(
                null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros
        );
        SourceRecord transformedRecord = smt.apply(record);
        Assertions.assertEquals(ChronoUnit.MILLIS.between(Instant.EPOCH, now), ((Struct)transformedRecord.value()).get("timestamp"));
    }

    @Test
    public void shouldConvertTimestampToStringMicroSecondsWithSchema() {
        String format = "yyyy-MM-dd HH:mm:ss.SSS";
        Map<String, String> properties = new HashMap<>();
        properties.put("field", "timestamp");
        properties.put("target.type", "string");
        properties.put("format", format);

        smt.configure(properties);

        Schema schemaWithTimestampMicros = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .field("timestamp", SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
                .optional()
                .build();

        Instant now = Instant.now();
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        Struct recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
                .put("id", (byte) 1)
                .put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        SourceRecord record = new SourceRecord(
                null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros
        );
        SourceRecord transformedRecord = smt.apply(record);
        Assertions.assertEquals(formatter.format(Date.from(now)), ((Struct) transformedRecord.value()).get("timestamp"));
    }

    @Test
    public void shouldConvertTimestampMicroSecondsWhenNoSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put("field", "timestamp");
        properties.put("target.type", "unix");

        smt.configure(properties);

        Instant now = Instant.now();

        Map<String, Object> dataWithoutSchema = new HashMap<>();
        dataWithoutSchema.put("id", (byte) 1);
        dataWithoutSchema.put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        SourceRecord record = new SourceRecord(
                null, null, "test", 0, null, dataWithoutSchema
        );
        SourceRecord transformedRecord = smt.apply(record);
        Assertions.assertEquals(ChronoUnit.MILLIS.between(Instant.EPOCH, now), ((Map) transformedRecord.value()).get("timestamp"));
    }
}
