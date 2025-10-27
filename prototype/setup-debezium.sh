curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg-messages-json2",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "cdc",
      "database.server.name": "pgserver1",
      "slot.name": "debezium_slot_json",
      "publication.name": "debezium_pub_json",
      "tombstones.on.delete": "false",
      "include.schema.changes": "false",

      "schema.include.list": "public",
      "table.include.list": "public.messages",

      "topic.prefix": "messages",

      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",

      "decimal.handling.mode": "double",
      "include.before.image": "always"
    }
  }'
