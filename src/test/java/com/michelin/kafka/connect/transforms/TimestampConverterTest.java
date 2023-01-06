package com.michelin.kafka.connect.transforms;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

class TimestampConverterTest {
    private final TimestampMicrosConverter.Value<SourceRecord> smt = new TimestampMicrosConverter.Value<>();

    @Test
    void shouldConvertTimestampMicroSecondsWithSchema() {
        final var properties = new HashMap<String, String>();
        properties.put("field", "timestamp");
        properties.put("target.type", "unix");

        smt.configure(properties);

        final var schemaWithTimestampMicros = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .field("timestamp", SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
                .optional()
                .build();

        final var now = Instant.now();

        final var recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
                .put("id", (byte) 1)
                .put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        final var record = new SourceRecord(
                null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros
        );
        final var transformedRecord = smt.apply(record);
        Assertions.assertEquals(ChronoUnit.MILLIS.between(Instant.EPOCH, now), ((Struct) transformedRecord.value()).get("timestamp"));
    }

    @Test
    void shouldConvertTimestampToStringMicroSecondsWithSchema() {
        final var format = "yyyy-MM-dd HH:mm:ss.SSS";
        final var properties = new HashMap<String, String>();
        properties.put("field", "timestamp");
        properties.put("target.type", "string");
        properties.put("format", format);

        smt.configure(properties);

        final var schemaWithTimestampMicros = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .field("timestamp", SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
                .optional()
                .build();

        final var now = Instant.now();
        final var formatter = new SimpleDateFormat(format);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        final var recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
                .put("id", (byte) 1)
                .put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        final var record = new SourceRecord(
                null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros
        );
        final var transformedRecord = smt.apply(record);
        Assertions.assertEquals(formatter.format(Date.from(now)), ((Struct) transformedRecord.value()).get("timestamp"));
    }

    @Test
    void shouldConvertTimestampMicroSecondsWhenNoSchema() {
        final var properties = new HashMap<String, String>();
        properties.put("field", "timestamp");
        properties.put("target.type", "unix");

        smt.configure(properties);

        final var now = Instant.now();

        final var dataWithoutSchema = new HashMap<String, Object>();
        dataWithoutSchema.put("id", (byte) 1);
        dataWithoutSchema.put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        final var record = new SourceRecord(
                null, null, "test", 0, null, dataWithoutSchema
        );
        final var transformedRecord = smt.apply(record);
        Assertions.assertEquals(ChronoUnit.MILLIS.between(Instant.EPOCH, now), ((Map) transformedRecord.value()).get("timestamp"));
    }
}
