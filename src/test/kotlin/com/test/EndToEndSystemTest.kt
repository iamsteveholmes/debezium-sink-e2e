package com.test

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.KafkaContainer
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.util.*
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds


@ExtendWith(ContainerExtension::class)
class EndToEndSystemTest {

    @Test
    fun canRegisterConnection() {

        val sinkConnectorConfiguration = sinkConnectorConfiguration(sinkContainer)

        val sourceConnectorConfiguration = sourceConnectorConfiguration()

        logger.error("starting connections")
        getConnection(sinkContainer).use { connection ->
            logger.error("Creating Sink Table")
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                                CREATE TABLE DEBEZIUM.ACCOUNT (ID NVARCHAR2(18) not null primary key ,
                                title VARCHAR2(255))
                            """
                )
            }
        }
        getConnection(sourceContainer).use { connection ->
            logger.error("Updating Source table and records")
            connection.createStatement().use { statement ->
                logger.error("Created statement")
                statement.execute(
                    """
                                CREATE TABLE debezium.account (ID NVARCHAR2(18) not null primary key,
                                title VARCHAR2(255))
                            """
                )
                statement.execute("""ALTER TABLE debezium.account ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS""")
                logger.error("execute: ${statement.executeUpdate("insert into debezium.account (id, title) values ('1', 'Learn CDC')")}")
                logger.error("execute: ${statement.executeUpdate("insert into debezium.account (id, title) values ('2', 'Learn Debezium')")}")
                val result = statement.executeQuery("select * from debezium.account")
                while (result.next()) {
                    logger.error(result.getString("title"))
                }
            }
        }
        kcatConsume()
        logger.error("inserted")
        debeziumContainer.registerConnector(
            "sink-connector",
            sinkConnectorConfiguration
        )
        debeziumContainer.registerConnector(
            "source-connector",
            sourceConnectorConfiguration
        )
//        getConsumer(kafkaContainer).use { consumer ->
//            consumer.listTopics().forEach { topic ->
//                logger.error("topic.key: ${topic.key}")
//                logger.error("topic.value: ${topic.value}")
//            }
//            consumer.subscribe(listOf("todo.DEBEZIUM.ACCOUNT"))
//            val changeEvents: List<ConsumerRecord<String, String>> =
//                drain(consumer)
//
//            changeEvents.forEach { logger.error("changeEvent = $${it}") }
//        }
        getConnection(sinkContainer).use { connection ->
            connection.createStatement().use { statement ->
                logger.error("Reading sink records")
                val results = mutableListOf<String>()
                for (i in 0 until 20) {
                    val resultSet = statement.executeQuery("""select * from DEBEZIUM.ACCOUNT""")
                    val titles = resultSet.use {
                        generateSequence {
                            if (resultSet.next()) resultSet.getString("title") else null
                        }.toList()
                    }
                    if (titles.containsAll(listOf("Learn CDC", "Learn Debezium"))) {
                        results.addAll(titles)
                        break
                    } else {
                        runBlocking { delay(1.seconds) }
                    }
                }
                assertTrue {
                    results.isNotEmpty()
                }
            }
        }
    }
}

fun getConnection(container: JdbcDatabaseContainer<*>): Connection {
    logger.error("jdbcUrl: ${container.jdbcUrl}")
    return DriverManager.getConnection(
        container.jdbcUrl,
        container.username,
        container.password
    )
}

private fun getConsumer(
    kafkaContainer: KafkaContainer
): KafkaConsumer<String, String> {
    return KafkaConsumer(
        mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "tc-${UUID.randomUUID()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        ),
        StringDeserializer(),
        StringDeserializer()
    )
}

private fun getProducer(
    kafkaContainer: KafkaContainer
): KafkaProducer<String, String> {
    return KafkaProducer<String, String>(
        mutableMapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ProducerConfig.CLIENT_ID_CONFIG to "${UUID.randomUUID()}",
        ),
        StringSerializer(),
        StringSerializer()
    )
}

private fun drain(
    consumer: KafkaConsumer<String, String>
): List<ConsumerRecord<String, String>> {
    val allRecords: MutableList<ConsumerRecord<String, String>> = ArrayList()

    runBlocking {
        launch {
            for (i in 0 until 30) {
                logger.error("DRAIN POLL $i")
                consumer.poll(Duration.ofMillis(500))
                    .iterator()
                    .forEachRemaining { e: ConsumerRecord<String, String> ->
                        logger.error("added $e")
                        allRecords.add(e)
                    }
                if (allRecords.size == 2) {
                    break
                }
            }
        }
    }
    return allRecords
}

private fun kcatConsume() {
    CoroutineScope(Dispatchers.IO).launch {
        for (i in 0 until 40) {
            val stdout: String = kcatContainer
                .execInContainer("kcat", "-b", "${kafkaContainer.containerName.removePrefix("/")}:9092", "-C", "-t", "todo.DEBEZIUM.ACCOUNT", "-e")
                .stdout
            if(stdout.isNotEmpty()) {
                logger.error("kcat message = $stdout")
            }
            delay(1.seconds)
        }
    }
}
