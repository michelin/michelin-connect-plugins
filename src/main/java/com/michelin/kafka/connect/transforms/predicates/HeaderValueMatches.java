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
package com.michelin.kafka.connect.transforms.predicates;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Kafka Connect custom header value filter.
 *
 * @param <R> Type of Kafka record.
 * @author Michelin
 */
public class HeaderValueMatches<R extends ConnectRecord<R>> implements Predicate<R> {
    public static final String OVERVIEW_DOC =
            "A predicate which is true for records with a header's value that matches the configured regular expression.";

    private static final String HEADER_CONFIG = "header.name";
    private static final String PATTERN_CONFIG = "pattern";
    private static final String MISSING_HEADER_CONFIG = "missing.header.behavior";

    /** Definition of accepted parameters: header.name, pattern and missing.header.behavior. */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    HEADER_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM,
                    "The header name.")
            .define(
                    PATTERN_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString(), new RegexValidator()),
                    ConfigDef.Importance.MEDIUM,
                    "A Java regular expression for matching against the value of a record's header.")
            .define(
                    MISSING_HEADER_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    "false",
                    ConfigDef.Importance.LOW,
                    "Predicate behavior when header is missing [true/false]. Default to false");

    private String headerName;
    private Pattern pattern;
    private boolean missingHeaderBehavior;

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
    public void configure(final Map<String, ?> pConfigs) {
        final var config = new SimpleConfig(CONFIG_DEF, pConfigs);
        this.headerName = config.getString(HEADER_CONFIG);
        this.pattern = Pattern.compile(config.getString(PATTERN_CONFIG));
        this.missingHeaderBehavior = config.getBoolean(MISSING_HEADER_CONFIG);
    }

    @Override
    public boolean test(final R pRecord) {
        final var headerIterator = pRecord.headers().allWithName(headerName);
        // Header not found
        if (headerIterator == null || !headerIterator.hasNext()) {
            return missingHeaderBehavior;
        }
        // Loop over headers (multiple with same name allowed)
        while (headerIterator.hasNext()) {
            final var header = headerIterator.next();
            final Object valueAsObject = header.value();
            final String valueAsString;
            if (valueAsObject != null) {
                if (valueAsObject instanceof byte[]) {
                    valueAsString = new String((byte[]) valueAsObject, StandardCharsets.UTF_8);
                } else {
                    valueAsString = valueAsObject.toString();
                }
            } else {
                valueAsString = null;
            }
            if (valueAsString != null && pattern.matcher(valueAsString).matches()) {
                return true;
            }
        }
        // No header value matches pattern
        return false;
    }

    @Override
    public void close() {
        // Nothing to clean
    }

    @Override
    public String toString() {
        return "HasHeader{" + "headerName='"
                + headerName + "'," + "pattern='"
                + pattern + "'," + "missingHeaderBehavior='"
                + missingHeaderBehavior + "'}";
    }
}
