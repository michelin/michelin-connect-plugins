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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect Single Message Transform (SMT) that takes an existing JSON header and expands each key-value pair into
 * separate individual Kafka message headers. The original JSON header is removed after expansion.
 *
 * <p>This transform is useful when you have JSON content in a header that you want to split into multiple headers for
 * better message routing and filtering.
 *
 * <p>Configuration:
 *
 * <ul>
 *   <li>header.field: Header name containing the JSON map (default: "headers")
 * </ul>
 */
public class ExpandJsonHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ExpandJsonHeaders.class);

    private static final String HEADER_FIELD_CONFIG = "header.field";
    private static final String HEADER_FIELD_DEFAULT = "headers";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    HEADER_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    HEADER_FIELD_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    "Header name containing the JSON map");

    private String headerField;
    private ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerField = config.getString(HEADER_FIELD_CONFIG);
        objectMapper = new ObjectMapper();

        log.info("Configured ExpandJsonHeaders with field='{}'", headerField);
    }

    /**
     * Applies the transformation to expand a JSON header into individual headers. Extracts key-value pairs from the
     * specified JSON header field, adds them as separate headers, and removes the original JSON header. If the header
     * is missing or invalid, the original record is returned unchanged.
     *
     * @param currentRecord the Kafka Connect record to transform
     * @return the transformed record with expanded headers
     */
    @Override
    public R apply(R currentRecord) {
        Headers headers = currentRecord.headers();

        try {
            Header headerValue = headers.allWithName(headerField).next();

            JsonNode jsonNode = objectMapper.readTree(headerValue.value().toString());

            if (!jsonNode.isObject()) {
                log.warn("Field '{}' is not a JSON object, skipping header extraction", headerField);
                return currentRecord;
            }

            jsonNode.properties().forEach(field -> {
                String headerName = field.getKey();
                String headerValueStr = field.getValue().asText();
                headers.addString(headerName, headerValueStr);
                log.debug("Added header: {} = {}", headerName, headerValueStr);
            });

            log.debug("Successfully extracted headers from field '{}'", headerField);

            // Remove the original JSON header after expansion
            headers.remove(headerField);
            log.debug("Removed original header '{}'", headerField);
        } catch (NoSuchElementException e) {
            log.debug("No '{}' field found in currentRecord, skipping header extraction", headerField);
            // Return original currentRecord if field is not found
            return currentRecord;
        } catch (Exception e) {
            log.warn("Failed to parse JSON from field '{}': {}", headerField, e.getMessage());
            // Return original currentRecord on parsing errors
            return currentRecord;
        }

        return currentRecord.newRecord(
                currentRecord.topic(),
                currentRecord.kafkaPartition(),
                currentRecord.keySchema(),
                currentRecord.key(),
                currentRecord.valueSchema(),
                currentRecord.value(),
                currentRecord.timestamp(),
                headers);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to close
    }
}
