package com.michelin.kafka.connect.transforms.predicates;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class HeaderValueMatches<R extends ConnectRecord<R>> implements Predicate<R> {

    private static final String HEADER_CONFIG = "header.name";
    private static final String PATTERN_CONFIG = "pattern";
    private static final String MISSING_HEADER_CONFIG = "missing.header.behavior";

    public static final String OVERVIEW_DOC = "A predicate which is true for records with a header's value that matches the configured regular expression.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                    "The header name.")
            .define(PATTERN_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString(), new RegexValidator()),
                    ConfigDef.Importance.MEDIUM,
                    "A Java regular expression for matching against the value of a record's header.")
            .define(MISSING_HEADER_CONFIG, ConfigDef.Type.BOOLEAN, "false", ConfigDef.Importance.LOW,
                    "Predicate behavior when header is missing [true/false]. Default to false");

    private String headerName;
    private Pattern pattern;
    private boolean missingHeaderBehavior;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.headerName = config.getString(HEADER_CONFIG);
        this.pattern = Pattern.compile(config.getString(PATTERN_CONFIG));
        this.missingHeaderBehavior = config.getBoolean(MISSING_HEADER_CONFIG);
    }


    @Override
    public boolean test(R record) {
        Iterator<Header> headerIterator = record.headers().allWithName(headerName);
        // Header not found
        if (headerIterator == null || !headerIterator.hasNext()) {
            return missingHeaderBehavior;
        }
        // Loop over headers (multiple with same name allowed)
        while (headerIterator.hasNext()) {
            Header header = headerIterator.next();
            if (header.value() != null && pattern.matcher(header.value().toString()).matches()) {
                return true;
            }
        }
        // No header value matches pattern
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "HasHeader{" +
                "headerName='" + headerName + "'," +
                "pattern='" + pattern + "'," +
                "missingHeaderBehavior='" + missingHeaderBehavior + "'" +
                '}';
    }
}
