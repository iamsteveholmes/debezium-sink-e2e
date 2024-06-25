package com.test

import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.debezium.testing.testcontainers.DebeziumContainer
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.*
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.images.builder.Transferable
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration


val logger: Logger = LoggerFactory.getLogger(ContainerExtension::class.java)
val logConsumer: Slf4jLogConsumer = Slf4jLogConsumer(logger)
private val network = Network.newNetwork()

val cdcorcl = DockerImageName.parse("cdcorcl:latest")
    .asCompatibleSubstituteFor("gvenzl/oracle-xe")
val sourceContainer = TodoOracleContainer()
    .withNetwork(network)
    .withUsername("debezium")
    .withPassword("dbz")
    .withNetworkAliases("oracle")
    .withCopyToContainer(
        MountableFile.forClasspathResource("setup-logminer.sh"),
        "/container-entrypoint-initdb.d/setup-logminer.sh"
    )
//    ).withLogConsumer(logConsumer).withStartupTimeout(3.minutes.toJavaDuration())

//val postgresContainer: PostgreSQLContainer<*> = PostgreSQLContainer(
//    DockerImageName.parse("quay.io/debezium/postgres:15")
//        .asCompatibleSubstituteFor("postgres")
//)
//    .withNetwork(network)
//    .withNetworkAliases("postgres")
//    .withLogConsumer(logConsumer)

val kafkaContainer = TodoKafkaContainer()
    .withNetwork(network)
//    .withEnv("LOG_LEVEL", "DEBUG")
//    .withLogConsumer(logConsumer)

val sinkContainer: OracleContainer =
    OracleContainer(DockerImageName.parse("gvenzl/oracle-xe").withTag("21.3.0-slim-faststart"))
        .withNetwork(network)
        .withNetworkAliases("oracle-sink")
        .withUsername("debezium")
        .withPassword("dbz")
//        .withLogConsumer(logConsumer) as OracleContainer

val debeziumContainer = DebeziumContainer.latestStable()
    .withNetwork(network)
//    .withKafka(kafkaContainer.network, "${kafkaContainer.containerName.removePrefix("/")}:9092")
    .withKafka(kafkaContainer)
    .dependsOn(kafkaContainer)
    .dependsOn(sourceContainer)
    .withEnv("LOG_LEVEL", "TRACE")
    .withLogConsumer(logConsumer) as DebeziumContainer

val kcatContainer = GenericContainer("confluentinc/cp-kcat:7.6.1")
    .withCreateContainerCmdModifier { cmd ->
        cmd.withEntrypoint("sh")
    }
    .withCopyToContainer(Transferable.of("Message produced by kcat"), "/data/msgs.txt")
    .withNetwork(network)
    .withCommand("-c", "tail -f /dev/null")


fun sinkConnectorConfiguration(databaseContainer: JdbcDatabaseContainer<*>): ConnectorConfiguration {
    return ConnectorConfiguration.create().apply {
        with("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector")
        with("tasks.max", "1")
        with("connection.url", "jdbc:oracle:thin:@${databaseContainer.containerName.removePrefix("/")}:1521/xepdb1")
        with("connection.username", databaseContainer.username)
        with("connection.password", databaseContainer.password)
        with("insert.mode", "upsert")
        with("primary.key.mode", "record_value")
        with("primary.key.fields", "ID")
        with("topics", "todo.DEBEZIUM.ACCOUNT")
        with("table.format", "\${source.table}")
//        with("delete.enabled", "true")
//        with("schema.evolution", "basic")
//        with("database.time_zone", "UTC")
//        with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
//        with("key.converter.schemas.enable", "true")
//        with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
//        with("value.converter.schemas.enable", "true")
    }
}

fun sourceConnectorConfiguration(): ConnectorConfiguration {
    return ConnectorConfiguration
        .forJdbcContainer(sourceContainer).apply {
            with("topic.prefix", "todo")
            with("database.user", "c##dbzuser")
            with("database.password", "dbz")
            with(
                "schema.history.internal.kafka.bootstrap.servers",
                "${kafkaContainer.containerName.removePrefix("/")}:9092"
            )
            with("schema.history.internal.kafka.topic", "schema-changes.todo")
            with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            with("key.converter.schemas.enable", "true")
            with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            with("value.converter.schemas.enable", "true")
            with("database.dbname", "xe")
            with("database.pdb.name", "xepdb1")
            with("schema.history.internal.store.only.captured.tables.ddl", "true")
            with("schema.history.internal.store.only.captured.databases.ddl", "true")
            with("table.include.list", "DEBEZIUM.ACCOUNT")
            with("log.mining.strategy", "online_catalog")
        }
}

class ContainerExtension : BeforeAllCallback, AfterAllCallback {

    override fun beforeAll(context: ExtensionContext?) {
        Startables.deepStart(kafkaContainer, sourceContainer, sinkContainer, debeziumContainer, kcatContainer).join()
            .also {
                logger.error("containers started")
            }
    }

    override fun afterAll(context: ExtensionContext?) {
        // Do nothing
    }
}

class TodoOracleContainer : OracleContainer(cdcorcl) {
    override fun getDriverClassName(): String {
        return "oracle.jdbc.OracleDriver"
    }
}

class TodoKafkaContainer : KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
//class TodoKafkaContainer : KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))
