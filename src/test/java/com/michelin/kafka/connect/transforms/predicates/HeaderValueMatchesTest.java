package com.michelin.kafka.connect.transforms.predicates;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class HeaderValueMatchesTest {
    private HeaderValueMatches<SourceRecord> predicate = new HeaderValueMatches<>();

    @Test
    public void ValueMatches() {
        final Map<String, Object> props = new HashMap<>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("my-header", "fixed", null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertTrue(result);
    }

    @Test
    public void ValueRegexMatches() {
        final Map<String, Object> props = new HashMap<>();

        props.put("header.name", "license-plate");
        props.put("pattern", "[A-Z]{2}-[0-9]{3}-[A-Z]{2}");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("license-plate", "CG-768-AP", null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertTrue(result);
    }

    @Test
    public void valueNull() {
        final Map<String, Object> props = new HashMap<>();
        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("my-header", null, null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertFalse(result);
    }

    @Test
    public void ValueNotMatching() {
        final Map<String, Object> props = new HashMap<>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("my-header", "OTHER", null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertFalse(result);
    }

    @Test
    public void MissingHeaderDefaultBehavior() {
        final Map<String, Object> props = new HashMap<>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("other-header", "OTHER", null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertFalse(result);
    }

    @Test
    public void MissingHeaderOverriddenBehavior() {
        final Map<String, Object> props = new HashMap<>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");
        props.put("missing.header.behavior", "true");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("other-header", "OTHER", null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertTrue(result);
    }

    @Test
    public void MultipleHeadersWithMatchingValue() {
        final Map<String, Object> props = new HashMap<>();

        props.put("header.name", "my-header");
        props.put("pattern", "fixed");
        props.put("missing.header.behavior", "true");

        predicate.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("my-header", "OTHER", null);
        headers.add("my-header", "DIFFERENT", null);
        headers.add("my-header", "fixed", null);
        headers.add("unrelated-header", null, null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, null, 0L, headers);

        final boolean result = predicate.test(record);

        assertTrue(result);
    }
}
