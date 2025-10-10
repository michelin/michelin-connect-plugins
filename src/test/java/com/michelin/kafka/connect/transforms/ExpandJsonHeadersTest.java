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

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExpandJsonHeadersTest {

    private ExpandJsonHeaders<SourceRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new ExpandJsonHeaders<>();
    }

    @Test
    void testConfigureWithDefaults() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        assertNotNull(transform);
    }

    @Test
    void testConfigureWithCustomValues() {
        Map<String, Object> config = new HashMap<>();
        config.put("header.field", "custom_headers");

        transform.configure(config);

        assertNotNull(transform);
    }

    @Test
    void testExpandJsonHeaderWithMultipleFields() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create record with JSON header
        Headers headers = new ConnectHeaders();
        headers.addString("headers", "{\"userId\":\"user123\",\"requestId\":\"req456\",\"source\":\"web\"}");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Verify expanded headers were added
        Headers transformedHeaders = transformed.headers();
        assertNotNull(transformedHeaders);

        assertEquals("user123", getHeaderValue(transformedHeaders, "userId"));
        assertEquals("req456", getHeaderValue(transformedHeaders, "requestId"));
        assertEquals("web", getHeaderValue(transformedHeaders, "source"));

        // Verify original header was removed
        assertNull(getHeaderValue(transformedHeaders, "headers"));
    }

    @Test
    void testExpandJsonHeaderWithCustomFieldName() {
        // Configure transform with custom field name
        Map<String, Object> config = new HashMap<>();
        config.put("header.field", "metadata");
        transform.configure(config);

        // Create record with JSON header using custom field name
        Headers headers = new ConnectHeaders();
        headers.addString("metadata", "{\"version\":\"1.0\",\"type\":\"event\"}");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Verify headers were expanded
        Headers transformedHeaders = transformed.headers();
        assertNotNull(transformedHeaders);

        assertEquals("1.0", getHeaderValue(transformedHeaders, "version"));
        assertEquals("event", getHeaderValue(transformedHeaders, "type"));

        // Verify original header was removed
        assertNull(getHeaderValue(transformedHeaders, "metadata"));
    }

    @Test
    void testExpandEmptyJsonHeader() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create record with empty JSON header
        Headers headers = new ConnectHeaders();
        headers.addString("headers", "{}");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Should have removed the original header even if empty
        Headers transformedHeaders = transformed.headers();
        assertNotNull(transformedHeaders);
        assertNull(getHeaderValue(transformedHeaders, "headers"));
    }

    @Test
    void testMissingHeaderField() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create record without the expected header
        Headers headers = new ConnectHeaders();
        headers.addString("other_header", "some_value");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Should return original record unchanged
        assertEquals(record.value(), transformed.value());
        assertEquals("some_value", getHeaderValue(transformed.headers(), "other_header"));
    }

    @Test
    void testInvalidJsonHeader() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create record with invalid JSON
        Headers headers = new ConnectHeaders();
        headers.addString("headers", "invalid json {");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Should not throw exception and should return original record
        assertNotNull(transformed);
        assertEquals(record.value(), transformed.value());
        // Original header should still be present since parsing failed
        assertEquals("invalid json {", getHeaderValue(transformed.headers(), "headers"));
    }

    @Test
    void testNonObjectJsonHeader() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create record with JSON array instead of object
        Headers headers = new ConnectHeaders();
        headers.addString("headers", "[\"item1\", \"item2\"]");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Should not expand array and should return original record
        assertNotNull(transformed);
        assertEquals(record.value(), transformed.value());
        // Original header should still be present since it's not an object
        assertEquals("[\"item1\", \"item2\"]", getHeaderValue(transformed.headers(), "headers"));
    }

    @Test
    void testComplexJsonValues() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create record with complex JSON values (nested objects, arrays, etc.)
        Headers headers = new ConnectHeaders();
        headers.addString(
                "headers", "{\"simple\":\"value\",\"number\":42,\"boolean\":true,\"nested\":{\"key\":\"value\"}}");

        SourceRecord record = new SourceRecord(
                null, null, "test-topic", null, null, null, Schema.STRING_SCHEMA, "test-value", null, headers);

        // Apply transform
        SourceRecord transformed = transform.apply(record);

        // Verify all values are converted to strings
        Headers transformedHeaders = transformed.headers();
        assertNotNull(transformedHeaders);

        assertEquals("value", getHeaderValue(transformedHeaders, "simple"));
        assertEquals("42", getHeaderValue(transformedHeaders, "number"));
        assertEquals("true", getHeaderValue(transformedHeaders, "boolean"));
        // For nested objects, Jackson's asText() returns the toString() representation
        assertNotNull(getHeaderValue(transformedHeaders, "nested")); // Just check it exists

        // Verify original header was removed
        assertNull(getHeaderValue(transformedHeaders, "headers"));
    }

    private String getHeaderValue(Headers headers, String key) {
        for (Header header : headers) {
            if (header.key().equals(key)) {
                return (String) header.value();
            }
        }
        return null;
    }
}
