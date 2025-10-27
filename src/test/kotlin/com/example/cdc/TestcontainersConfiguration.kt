package com.example.cdc

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.StandardCharsets

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    private val network: Network = Network.newNetwork()

    @Bean
    @ServiceConnection
    fun kafkaContainer(): KafkaContainer {
        return KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
    }

    @Bean
    @ServiceConnection
    fun postgresContainer(): PostgreSQLContainer<*> {
        return PostgreSQLContainer(DockerImageName.parse("postgres:latest"))
            .withDatabaseName("cdc")
            .withUsername("postgres")
            .withPassword("postgres")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withCommand(
                "postgres",
                "-c", "wal_level=logical",
                "-c", "max_wal_senders=20",
                "-c", "max_replication_slots=20",
                "-c", "max_connections=200"
            )
    }

    @Bean
    fun debeziumConnectContainer(kafka: KafkaContainer, postgres: PostgreSQLContainer<*>): GenericContainer<*> {
        val connect = GenericContainer(DockerImageName.parse("debezium/connect:3.0.0.Final"))
            .withNetwork(network)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:9093")
            .withEnv("GROUP_ID", "1")
            .withEnv("CONFIG_STORAGE_TOPIC", "connect_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "connect_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "connect_statuses")
            .withEnv("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .dependsOn(kafka, postgres)
            .waitingFor(Wait.forHttp("/"))

        Startables.deepStart(setOf(kafka, postgres, connect)).join()
        registerPostgresConnector(connect)

        return connect
    }

    private fun registerPostgresConnector(connect: GenericContainer<*>) {
        val connectorConfig = (
            """
            {
              "name": "pg-messages-json",
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
            }
            """
            ).trimIndent()

        val url = URI("http://${connect.host}:${connect.getMappedPort(8083)}/connectors").toURL()
        val conn = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            setRequestProperty("Content-Type", "application/json")
            doOutput = true
        }

        conn.outputStream.use { os ->
            val input = connectorConfig.toByteArray(StandardCharsets.UTF_8)
            os.write(input)
        }

        val responseCode = conn.responseCode
        if (responseCode != 201) {
            val errorBody = try {
                conn.errorStream?.readBytes()?.toString(StandardCharsets.UTF_8)
            } catch (_: Exception) { null }
            throw IllegalStateException("Failed to create Debezium connector: HTTP $responseCode $errorBody")
        }
        conn.disconnect()
    }

}
