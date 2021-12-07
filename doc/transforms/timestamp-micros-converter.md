# TimestampMicrosConverter

Certain producers may choose to represent time using unix time.  
Avro supports 3 logical types:
- ``timestamp-millis`` stores the number of milliseconds from the unix epoch
- ``timestamp-micros`` stores the number of microseconds from the unix epoch

Currently, the [`TimestampConverter`](https://docs.confluent.io/current/connect/transforms/timestampconverter.html) only supports the milliseconds precision.  
`TimestampMicrosConverter` SMT is a modification of `TimestampComverter` which use a microseconds as default timestamp precsion rather than milliseconds.

## Usage
```
"transforms": "convertEventDate",
"transforms.convertEventDate.type": "com.michelin.kafka.connect.transforms.qlik.replicate.ReplicateTimestampConverter$Value",
"transforms.convertEventDate.field": "event_",
"transforms.convertEventDate.target.type": "Timestamp",
```

## Documentation
Check the [`TimestampConverter`](https://docs.confluent.io/current/connect/transforms/timestampconverter.html) documentation.