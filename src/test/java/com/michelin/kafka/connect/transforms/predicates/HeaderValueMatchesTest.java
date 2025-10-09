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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class HeaderValueMatchesTest {
    private final HeaderValueMatches<SourceRecord> predicate = new HeaderValueMatches<>();

    @Test
    void ValueMatches() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("my-header", "fixed", null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertTrue(predicate.test(record));
    }

    @Test
    void ValueRegexMatches() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "license-plate");
        props.put("pattern", "[A-Z]{2}-[0-9]{3}-[A-Z]{2}");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("license-plate", "CG-768-AP", null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertTrue(predicate.test(record));
    }

    @Test
    void ByteArrayValueRegexMatches() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "license-plate");
        props.put("pattern", "[A-Z]{2}-[0-9]{3}-[A-Z]{2}");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("license-plate", "CG-768-AP".getBytes(StandardCharsets.UTF_8), null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertTrue(predicate.test(record));
    }

    @Test
    void valueNull() {
        final var props = new HashMap<String, Object>();
        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("my-header", null, null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertFalse(predicate.test(record));
    }

    @Test
    void ValueNotMatching() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("my-header", "OTHER", null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertFalse(predicate.test(record));
    }

    @Test
    void MissingHeaderDefaultBehavior() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("other-header", "OTHER", null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertFalse(predicate.test(record));
    }

    @Test
    void MissingHeaderOverriddenBehavior() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");
        props.put("missing.header.behavior", "true");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("other-header", "OTHER", null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertTrue(predicate.test(record));
    }

    @Test
    void MultipleHeadersWithMatchingValue() {
        final var props = new HashMap<String, Object>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");
        props.put("missing.header.behavior", "true");

        predicate.configure(props);

        final var headers = new ConnectHeaders();
        headers.add("my-header", "OTHER", null);
        headers.add("my-header", "DIFFERENT", null);
        headers.add("my-header", "fixed", null);
        headers.add("unrelated-header", null, null);

        final var record = new SourceRecord(null, null, "test", 0, null, null, null, null, 0L, headers);

        assertTrue(predicate.test(record));
    }
}
