# HeaderValueMatches Predicate
A Kafka Connect [Predicate](https://cwiki.apache.org/confluence/display/KAFKA/KIP-585%3A+Filter+and+Conditional+SMTs) to be used to conditionally apply a Transformation by configuring the transformation's predicate (and negate) configuration parameters. In particular, the Filter transformation can be conditionally applied in order to filter certain records from further processing.

`HeaderValueMatches` predicate will check the Kafka Record's headers against a Regexp. 

## Documentation

|Property name|Default| Description|
|---|---|---|
|`header.name`| | The header name |
|`pattern`| | The regular expression to match against the value of `header.name` |
|`missing.header.behavior`|`false`|Predicate behavior when header is missing |

## Usage
### Single header filter
This will **drop** records with header ``h1`` in ``[foo,bar]``
````json
{
  "name": "jdbc_source_mysql_01",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://mysql:3306/test",
    ...
    "table.whitelist" : "test.accounts",

    "transforms": "filter-h1",
    "transforms.filter-h1.type": "org.apache.kafka.connect.transforms.Filter",
    "transforms.filter-h1.predicate": "check-header-h1",
    
    "predicates": "check-header-h1",
    "predicates.check-header-h1.type": "com.michelin.kafka.connect.transforms.predicates.HeaderValueMatches",
    "predicates.check-header-h1.header.name": "h1",
    "predicates.check-header-h1.pattern": "foo|bar"
  }
}
````

### Chained filters predicates
This will **keep** only records with header ``h1`` in ``[foo,bar]`` and header ``h2`` in ``[fizz,buzz]``
````json
{
  ...
  "transforms": "filter-h1, filter-h2",   
  "transforms.filter-h1.type": "Filter",
  "transforms.filter-h1.predicate": "check-header-h1",
  "transforms.filter-h1.negate": "true", // negate Filter = keep
  "transforms.filter-h2.type": "Filter",
  "transforms.filter-h2.predicate": "check-header-h2",
  "transforms.filter-h2.negate": "true",
    
  "predicates": "check-header-h1,check-header-h2",
  "predicates.check-header-h1.type": "HeaderValueMatches",
  "predicates.check-header-h1.header.name": "h1",
  "predicates.check-header-h1.pattern": "foo|bar",
  "predicates.check-header-h2.type": "HeaderValueMatches",
  "predicates.check-header-h2.header.name": "h2",
  "predicates.check-header-h2.pattern": "fizz|buzz"
}
````