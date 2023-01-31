# aes-256-config-provider
AES256 ConfigProvider for Apache Kafka Connect.  
Nice to have when using Kafka Connect in a multi-tenant environment.

## Worker configuration

1. Edit connect-distributed.properties
    ````properties
    # connect-distributed.properties
    config.providers=aes256
    config.providers.aes256.class=com.michelin.kafka.AES256ConfigProvider
    config.providers.aes256.param.key=0000111122223333
    config.providers.aes256.param.salt=0000111122223333
    ````  
   
2. Use NS4Kafka to encrypt your password, or provide an API to your users so that they can encrypt their passwords with your key
   * NS4Kafka: https://github.com/michelin/kafkactl/blob/main/README.md
   * AES256 API: https://github.com/twobeeb/aes-256-vault-api  


3. Keep your key safe !

## Connector configuration

Encode your password using the AES256 API:
````console
curl -X POST http://admin-api/vault -d '{"password": "mypassword"}'
> ${aes256:mfw43l96122yZiDhu2RevQ==}
````

Use the result in your Kafka Connect configurations:
````json
{
  "name": "jdbc_source_mysql_01",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://mysql:3306/test",
    "connection.user": "connect_user",
    "connection.password": "${aes256:mfw43l96122yZiDhu2RevQ==}",
    "topic.prefix": "mysql-01-",
    "poll.interval.ms" : 3600000,
    "table.whitelist" : "test.accounts",
    "mode":"bulk"
  }
}
````

Done.
