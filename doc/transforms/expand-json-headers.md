# Expand JSON Headers SMT

A Kafka Connect Single Message Transform (SMT) that takes an existing JSON header and expands each key-value pair into separate individual Kafka message headers. The original JSON header is removed after expansion.

## Use Case

This transform is particularly useful when working with transactional outbox patterns where you have JSON data in a Kafka header that you want to expand into individual headers for better message routing and filtering.

For example, if you have a Kafka header named `headers` containing:
```json
{"userId": "user123", "requestId": "req456", "source": "web"}
```

This SMT will:
1. Extract each key-value pair and create individual headers:
    - `userId` = "user123"
    - `requestId` = "req456"
    - `source` = "web"
2. Remove the original `headers` header

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `header.field` | String | `headers` | Header name containing the JSON map |

## Usage with Debezium EventRouter

```json
{
  "name": "outbox-connector-with-headers",  
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "localhost",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "plugin.name": "pgoutput",
    "table.include.list": "public.outbox_messages",
    "transforms": "outbox,addHeaders,expandHeaders",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.expand.json.payload": "true",
    "transforms.outbox.table.field.event.key": "partition_key",
    "transforms.outbox.route.by.field": "topic",
    "transforms.outbox.route.topic.replacement": "${routedByValue}",
    "transforms.outbox.table.fields.additional.placement": "headers:headers",
    "transforms.expandHeaders.type": "com.michelin.connect.transforms.ExpandJsonHeaders",
    "transforms.expandHeaders.header.field": "headers",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8085"
  }
}
```
## Examples

### Basic Usage
```json
"transforms": "expandHeaders",
"transforms.expandHeaders.type": "com.michelin.connect.transforms.ExpandJsonHeaders"
```

### With Custom Field Name
```json
"transforms": "expandHeaders",
"transforms.expandHeaders.type": "com.michelin.connect.transforms.ExpandJsonHeaders",
"transforms.expandHeaders.header.field": "metadata"
```

## Behavior

- **Input**: A Kafka header containing JSON map data
- **Processing**: Parses the JSON and creates individual headers for each key-value pair
- **Output**: Individual headers with the original JSON header removed
- **Error Handling**: If the header is missing, invalid JSON, or not a JSON object, the record is passed through unchanged

## Error Handling

The transform is designed to be resilient:
- If the specified header is missing, the record is passed through unchanged
- If the JSON is invalid, the record is passed through unchanged
- If the header is not a JSON object, the record is passed through unchanged
- Errors are logged but do not cause the connector to fail
