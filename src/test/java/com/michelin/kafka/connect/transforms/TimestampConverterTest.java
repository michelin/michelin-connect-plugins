/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kafka.connect.transforms;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
                .field(
                        "timestamp",
                        SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
                .optional()
                .build();

        final var now = Instant.now();

        final var recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
                .put("id", (byte) 1)
                .put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        final var record =
                new SourceRecord(null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros);
        final var transformedRecord = smt.apply(record);
        Assertions.assertEquals(
                ChronoUnit.MILLIS.between(Instant.EPOCH, now), ((Struct) transformedRecord.value()).get("timestamp"));
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
                .field(
                        "timestamp",
                        SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
                .optional()
                .build();

        final var now = Instant.now();
        final var formatter = new SimpleDateFormat(format);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        final var recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
                .put("id", (byte) 1)
                .put("timestamp", ChronoUnit.MICROS.between(Instant.EPOCH, now));

        final var record =
                new SourceRecord(null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros);
        final var transformedRecord = smt.apply(record);
        Assertions.assertEquals(
                formatter.format(Date.from(now)), ((Struct) transformedRecord.value()).get("timestamp"));
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

        final var record = new SourceRecord(null, null, "test", 0, null, dataWithoutSchema);
        final var transformedRecord = smt.apply(record);
        Assertions.assertEquals(
                ChronoUnit.MILLIS.between(Instant.EPOCH, now), ((Map) transformedRecord.value()).get("timestamp"));
    }
}
