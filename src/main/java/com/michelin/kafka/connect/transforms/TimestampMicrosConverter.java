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

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Fork from <a
 * href="https://github.com/apache/kafka/blob/trunk/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/TimestampConverter.java">in
 * progress Kafka TimestampConverter</a> to support timestamps microseconds by default.
 *
 * @param <R> Type of he record.
 * @author Michelin
 */
public abstract class TimestampMicrosConverter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Convert timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
                    + "Applies to individual fields or to the entire value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>"
                    + TimestampMicrosConverter.Key.class.getName() + "</code>) "
                    + "or value (<code>" + TimestampMicrosConverter.Value.class.getName() + "</code>).";

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";

    public static final String TARGET_TYPE_CONFIG = "target.type";

    public static final String FORMAT_CONFIG = "format";
    private static final String FORMAT_DEFAULT = "";

    public static final String UNIX_PRECISION_CONFIG = "unix.precision";
    private static final String UNIX_PRECISION_DEFAULT = "milliseconds";

    private static final String PURPOSE = "converting timestamp formats";
    private static final String TYPE_STRING = "string";
    private static final String TYPE_UNIX = "unix";
    private static final String TYPE_DATE = "Date";
    private static final String TYPE_TIME = "Time";
    private static final String TYPE_TIMESTAMP = "Timestamp";

    private static final String UNIX_PRECISION_MILLIS = "milliseconds";
    private static final String UNIX_PRECISION_MICROS = "microseconds";
    private static final String UNIX_PRECISION_NANOS = "nanoseconds";
    private static final String UNIX_PRECISION_SECONDS = "seconds";

    private static final Set<String> VALID_TYPES =
            new HashSet<>(Arrays.asList(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP));

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static final Schema OPTIONAL_DATE_SCHEMA =
            org.apache.kafka.connect.data.Date.builder().optional().schema();
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA =
            Timestamp.builder().optional().schema();
    public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();

    /** Definition of accepted parameters: field, target.type, format and unix.precision. */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    FIELD_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    "The field containing the timestamp, or empty if the entire value is a timestamp")
            .define(
                    TARGET_TYPE_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.ValidString.in(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP),
                    ConfigDef.Importance.HIGH,
                    "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
            .define(
                    FORMAT_CONFIG,
                    ConfigDef.Type.STRING,
                    FORMAT_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    "A SimpleDateFormat-compatible format for the timestamp. "
                            + "Used to generate the output when type=string or used to parse the input if the input is a string.")
            .define(
                    UNIX_PRECISION_CONFIG,
                    ConfigDef.Type.STRING,
                    UNIX_PRECISION_DEFAULT,
                    ConfigDef.ValidString.in(
                            UNIX_PRECISION_NANOS, UNIX_PRECISION_MICROS,
                            UNIX_PRECISION_MILLIS, UNIX_PRECISION_SECONDS),
                    ConfigDef.Importance.LOW,
                    "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. "
                            + "Used to generate the output when type=unix or used to parse the input if the input is a Long."
                            + "Note: This SMT will cause precision loss during conversions from, and to, values with sub-millisecond components.");

    private interface TimestampTranslator {
        /** Convert from the type-specific format to the universal java.util.Date format */
        Date toRaw(final Config pConfig, final Object pOrig);

        /** Get the schema for this format. */
        Schema typeSchema(final boolean pIsOptional);

        /** Convert from the universal java.util.Date format to the type-specific format */
        Object toType(final Config pConfig, final Date pOrig);
    }

    private static final Map<String, TimestampTranslator> TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(TYPE_STRING, new TimestampTranslator() {
            @Override
            public Date toRaw(final Config pConfig, final Object pOrig) {
                if (!(pOrig instanceof String)) {
                    throw new DataException("Expected string timestamp to be a String, but found " + pOrig.getClass());
                }
                try {
                    return pConfig.format.parse((String) pOrig);
                } catch (final ParseException e) {
                    throw new DataException(
                            "Could not parse timestamp: value (" + pOrig + ") does not match pattern ("
                                    + pConfig.format.toPattern() + ")",
                            e);
                }
            }

            @Override
            public Schema typeSchema(final boolean pIsOptional) {
                return pIsOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(final Config pConfig, final Date pOrig) {
                synchronized (pConfig.format) {
                    return pConfig.format.format(pOrig);
                }
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new TimestampTranslator() {
            @Override
            public Date toRaw(final Config pConfig, final Object pOrig) {
                if (!(pOrig instanceof Long)) {
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + pOrig.getClass());
                }
                long unixTime = (Long) pOrig;
                switch (pConfig.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return TimestampMicros.toLogical(TimestampMicros.SCHEMA, TimeUnit.SECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_MICROS:
                        return TimestampMicros.toLogical(
                                TimestampMicros.SCHEMA, TimeUnit.MICROSECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_NANOS:
                        return TimestampMicros.toLogical(
                                TimestampMicros.SCHEMA, TimeUnit.NANOSECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return TimestampMicros.toLogical(TimestampMicros.SCHEMA, unixTime);
                }
            }

            @Override
            public Schema typeSchema(final boolean pIsOptional) {
                return pIsOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(final Config pConfig, final Date pOrig) {
                return TimestampMicros.fromLogical(TimestampMicros.SCHEMA, pOrig);
            }
        });

        TRANSLATORS.put(TYPE_DATE, new TimestampTranslator() {
            @Override
            public Date toRaw(final Config pConfig, final Object pOrig) {
                if (!(pOrig instanceof Date)) {
                    throw new DataException("Expected Date to be a java.util.Date, but found " + pOrig.getClass());
                }
                // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
                return (Date) pOrig;
            }

            @Override
            public Schema typeSchema(final boolean pIsOptional) {
                return pIsOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Date toType(final Config pConfig, final Date pOrig) {
                final var result = Calendar.getInstance(UTC);
                result.setTime(pOrig);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, 0);
                result.set(Calendar.MILLISECOND, 0);
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIME, new TimestampTranslator() {
            @Override
            public Date toRaw(final Config pConfig, final Object pOrig) {
                if (!(pOrig instanceof Date)) {
                    throw new DataException("Expected Time to be a java.util.Date, but found " + pOrig.getClass());
                }
                // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
                return (Date) pOrig;
            }

            @Override
            public Schema typeSchema(final boolean pIsOptional) {
                return pIsOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
            }

            @Override
            public Date toType(final Config pConfig, final Date pOrig) {
                final var origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(pOrig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIMESTAMP, new TimestampTranslator() {
            @Override
            public Date toRaw(final Config pConfig, final Object pOrig) {
                if (!(pOrig instanceof Date)) {
                    throw new DataException("Expected Timestamp to be a java.util.Date, but found " + pOrig.getClass());
                }
                return (Date) pOrig;
            }

            @Override
            public Schema typeSchema(final boolean pIsOptional) {
                return pIsOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
            }

            @Override
            public Date toType(final Config pConfig, final Date pOrig) {
                return pOrig;
            }
        });
    }

    /**
     * This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
     * their behavior
     */
    private static final class Config {
        private final String field;
        private final String type;
        private final SimpleDateFormat format;
        private final String unixPrecision;

        private Config(
                final String pField, final String pType, final SimpleDateFormat pFormat, final String pUnixPrecision) {
            field = pField;
            type = pType;
            format = pFormat;
            unixPrecision = pUnixPrecision;
        }
    }

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(final Map<String, ?> pConfigs) {
        final var simpleConfig = new SimpleConfig(CONFIG_DEF, pConfigs);
        final var field = simpleConfig.getString(FIELD_CONFIG);
        final var type = simpleConfig.getString(TARGET_TYPE_CONFIG);
        final var formatPattern = simpleConfig.getString(FORMAT_CONFIG);
        final String unixPrecision = simpleConfig.getString(UNIX_PRECISION_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

        if (!VALID_TYPES.contains(type)) {
            throw new ConfigException("Unknown timestamp type in TimestampConverter: " + type + ". Valid values are "
                    + String.join(", ", VALID_TYPES) + ".");
        }
        if (type.equals(TYPE_STRING) && formatPattern.trim().isEmpty()) {
            throw new ConfigException(
                    "TimestampConverter requires format option to be specified when using string timestamps");
        }
        SimpleDateFormat format = null;
        if (formatPattern != null && !formatPattern.trim().isEmpty()) {
            try {
                format = new SimpleDateFormat(formatPattern);
                format.setTimeZone(UTC);
            } catch (IllegalArgumentException e) {
                throw new ConfigException(
                        "TimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps: "
                                + formatPattern,
                        e);
            }
        }
        config = new Config(field, type, format, unixPrecision);
    }

    @Override
    public R apply(final R pRecord) {
        if (operatingSchema(pRecord) == null) {
            return applySchemaless(pRecord);
        }
        return applyWithSchema(pRecord);
    }

    /**
     * Returns the definition of configuration parameters accepted by this plugin.
     *
     * @return Configuration definition.
     */
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Nothing to clean
    }

    public static class Key<R extends ConnectRecord<R>> extends TimestampMicrosConverter<R> {
        @Override
        protected Schema operatingSchema(final R pRecord) {
            return pRecord.keySchema();
        }

        @Override
        protected Object operatingValue(final R pRecord) {
            return pRecord.key();
        }

        @Override
        protected R newRecord(final R pRecord, final Schema pUpdatedSchema, final Object pUpdatedValue) {
            return pRecord.newRecord(
                    pRecord.topic(),
                    pRecord.kafkaPartition(),
                    pUpdatedSchema,
                    pUpdatedValue,
                    pRecord.valueSchema(),
                    pRecord.value(),
                    pRecord.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends TimestampMicrosConverter<R> {
        @Override
        protected Schema operatingSchema(final R pRecord) {
            return pRecord.valueSchema();
        }

        @Override
        protected Object operatingValue(final R pRecord) {
            return pRecord.value();
        }

        @Override
        protected R newRecord(final R pRecord, final Schema pUpdatedSchema, final Object pUpdatedValue) {
            return pRecord.newRecord(
                    pRecord.topic(),
                    pRecord.kafkaPartition(),
                    pRecord.keySchema(),
                    pRecord.key(),
                    pUpdatedSchema,
                    pUpdatedValue,
                    pRecord.timestamp());
        }
    }

    protected abstract Schema operatingSchema(final R pRecord);

    protected abstract Object operatingValue(final R pRecord);

    protected abstract R newRecord(final R pRecord, final Schema pUpdatedSchema, final Object pUpdatedValue);

    private R applyWithSchema(final R pRecord) {
        final var schema = operatingSchema(pRecord);
        if (config.field.isEmpty()) {
            final var value = operatingValue(pRecord);
            // New schema is determined by the requested target timestamp type
            final var updatedSchema = TRANSLATORS.get(config.type).typeSchema(schema.isOptional());
            return newRecord(pRecord, updatedSchema, convertTimestamp(value, timestampTypeFromSchema(schema)));
        }

        final var value = requireStructOrNull(operatingValue(pRecord), PURPOSE);
        var updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            final var builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            for (final Field field : schema.fields()) {
                if (field.name().equals(config.field)) {
                    builder.field(
                            field.name(),
                            TRANSLATORS
                                    .get(config.type)
                                    .typeSchema(field.schema().isOptional()));
                } else {
                    builder.field(field.name(), field.schema());
                }
            }
            if (schema.isOptional()) {
                builder.optional();
            }
            if (schema.defaultValue() != null) {
                final var updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                builder.defaultValue(updatedDefaultValue);
            }

            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }

        final var updatedValue = applyValueWithSchema(value, updatedSchema);
        return newRecord(pRecord, updatedSchema, updatedValue);
    }

    private Struct applyValueWithSchema(final Struct pValue, final Schema pUpdatedSchema) {
        if (pValue == null) {
            return null;
        }

        final var updatedValue = new Struct(pUpdatedSchema);
        for (final Field field : pValue.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(config.field)) {
                updatedFieldValue = convertTimestamp(pValue.get(field), timestampTypeFromSchema(field.schema()));
            } else {
                updatedFieldValue = pValue.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private R applySchemaless(final R pRecord) {
        final var rawValue = operatingValue(pRecord);
        if (rawValue == null || config.field.isEmpty()) {
            return newRecord(pRecord, null, convertTimestamp(rawValue));
        }

        final var value = requireMap(rawValue, PURPOSE);
        final var updatedValue = new HashMap<>(value);
        updatedValue.put(config.field, convertTimestamp(value.get(config.field)));
        return newRecord(pRecord, null, updatedValue);
    }

    /** Determine the type/format of the timestamp based on the schema */
    private static String timestampTypeFromSchema(final Schema pSchema) {
        if (Timestamp.LOGICAL_NAME.equals(pSchema.name())) {
            return TYPE_TIMESTAMP;
        } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(pSchema.name())) {
            return TYPE_DATE;
        } else if (Time.LOGICAL_NAME.equals(pSchema.name())) {
            return TYPE_TIME;
        } else if (pSchema.type() == Schema.Type.STRING) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TYPE_STRING;
        } else if (pSchema.type() == Schema.Type.INT64) {
            // If not otherwise specified, long == unix time
            return TYPE_UNIX;
        }
        throw new ConnectException("Schema " + pSchema + " does not correspond to a known timestamp type format");
    }

    /** Infer the type/format of the timestamp based on the raw Java type */
    private static String inferTimestampType(final Object pTimestamp) {
        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (pTimestamp instanceof Date) {
            return TYPE_TIMESTAMP;
        } else if (pTimestamp instanceof Long) {
            return TYPE_UNIX;
        } else if (pTimestamp instanceof String) {
            return TYPE_STRING;
        }
        throw new DataException(
                "TimestampConverter does not support " + pTimestamp.getClass() + " objects as timestamps");
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     *
     * @param pTimestamp the input timestamp, may be null
     * @param pTimestampFormat the format of the timestamp, or null if the format should be inferred
     * @return the converted timestamp
     */
    private Object convertTimestamp(final Object pTimestamp, final String pTimestampFormat) {
        if (pTimestamp == null) {
            return null;
        }
        final var timestampFormat = pTimestampFormat == null ? inferTimestampType(pTimestamp) : pTimestampFormat;
        final var sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        final var rawTimestamp = sourceTranslator.toRaw(config, pTimestamp);

        final var targetTranslator = TRANSLATORS.get(config.type);
        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + config.type);
        }
        return targetTranslator.toType(config, rawTimestamp);
    }

    private Object convertTimestamp(final Object pTimestamp) {
        return convertTimestamp(pTimestamp, null);
    }

    public static class TimestampMicros {
        public static final String LOGICAL_NAME = "com.michelin.kafka.connect.data.TimestampMicros";
        public static final Schema SCHEMA = builder().schema();

        public TimestampMicros() {
            // No specific
        }

        public static SchemaBuilder builder() {
            return SchemaBuilder.int64().name(LOGICAL_NAME).version(1);
        }

        public static long fromLogical(final Schema pSchema, final java.util.Date pValue) {
            if (!LOGICAL_NAME.equals(pSchema.name())) {
                throw new DataException(
                        "Requested conversion of TimestampMicros object but the schema does not match.");
            }
            return ChronoUnit.MILLIS.between(Instant.EPOCH, pValue.toInstant());
        }

        public static java.util.Date toLogical(final Schema pSchema, final long pValue) {
            if (!LOGICAL_NAME.equals(pSchema.name())) {
                throw new DataException(
                        "Requested conversion of TimestampMicros object but the schema does not match.");
            }
            return Date.from(Instant.EPOCH.plus(pValue, ChronoUnit.MICROS));
        }
    }
}
