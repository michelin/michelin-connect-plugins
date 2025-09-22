package com.michelin.kafka.connect.transforms;

import java.util.NoSuchElementException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.Map;

/**
 * <p>Kafka Connect Single Message Transform (SMT) that takes an existing JSON header
 * and expands each key-value pair into separate individual Kafka message headers.
 * The original JSON header is removed after expansion.</p>
 *
 * <p>This transform is useful when you have JSON content in a header that you want
 * to split into multiple headers for better message routing and filtering.</p>
 *
 * <p>Configuration:
 * <ul>
 *     <li>header.field: Header name containing the JSON map (default: "headers")</li>
 * </ul></p>
 */
public class ExpandJsonHeaders<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ExpandJsonHeaders.class);

    private static final String HEADER_FIELD_CONFIG = "header.field";
    private static final String HEADER_FIELD_DEFAULT = "headers";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(HEADER_FIELD_CONFIG, ConfigDef.Type.STRING, HEADER_FIELD_DEFAULT,
            ConfigDef.Importance.HIGH, "Header name containing the JSON map");

    private String headerField;
    private ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerField = config.getString(HEADER_FIELD_CONFIG);
        objectMapper = new ObjectMapper();

        log.info("Configured ExpandJsonHeaders with field='{}'", headerField);
    }

    @Override
    public R apply(R record) {
        Headers headers = record.headers();

        try {
            Header headerValue = headers.allWithName(headerField).next();

            JsonNode jsonNode = objectMapper.readTree(headerValue.value().toString());

            if (!jsonNode.isObject()) {
                log.warn("Field '{}' is not a JSON object, skipping header extraction", headerField);
                return record;
            }

            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String headerName = field.getKey();
                String headerValueStr = field.getValue().asText();
                headers.addString(headerName, headerValueStr);
                log.debug("Added header: {} = {}", headerName, headerValueStr);
            }

            log.debug("Successfully extracted headers from field '{}'", headerField);

            // Remove the original JSON header after expansion
            headers.remove(headerField);
            log.debug("Removed original header '{}'", headerField);
        } catch (NoSuchElementException e) {
            log.debug("No '{}' field found in record, skipping header extraction", headerField);
            // Return original record if field is not found
            return record;
        } catch (Exception e) {
            log.warn("Failed to parse JSON from field '{}': {}", headerField, e.getMessage());
            // Return original record on parsing errors
            return record;
        }

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            record.value(),
            record.timestamp(),
            headers
        );
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
