package no.nav.common.test.common

import com.nhaarman.mockito_kotlin.timeout
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.embeddedutils.ServerBase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import java.net.URL
import java.time.Duration
import java.util.Properties

// some kafka broker test utilities

val noOfBrokers: (AdminClient) -> Int = { it.describeCluster().nodes().get().toList().size }
val noOfTopics: (AdminClient) -> Int = { it.listTopics().names().get().toList().size }
val topics: (AdminClient) -> List<String> = { it.listTopics().names().get().toList() }

// some schema registry test utilities

const val SCHEMAREG_DefaultCompatibilityLevel = """{"compatibilityLevel":"BACKWARD"}"""
const val SCHEMAREG_NoSubjects = """[]"""

val scRegTests = mapOf(
        "should report default compatibility level" to Pair("/config", SCHEMAREG_DefaultCompatibilityLevel),
        "should report zero subjects" to Pair("/subjects", SCHEMAREG_NoSubjects)
)

suspend fun HttpClient.getSomething(endpoint: URL): String =
        this.get {
            url(endpoint)
            contentType(ContentType.Application.Json)
            timeout(500)
        }

val httpReqResp: (HttpClient, ServerBase, String) -> String = { client, sr, path ->
    try {
        runBlocking { client.getSomething(URL(sr.url + path)) }
    } catch (e: Exception) {
        e.javaClass.name
    }
}

// some kafka environment test utilities

fun createProducerACL(topic: String, user: String): List<AclBinding> {

    val resourcePattern = ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
    val principal = "User:$user"
    val host = "*"

    return listOf(
            AclBinding(
                    resourcePattern,
                    AccessControlEntry(
                            principal,
                            host,
                            AclOperation.DESCRIBE,
                            AclPermissionType.ALLOW
                    )
            ),
            AclBinding(
                    resourcePattern,
                    AccessControlEntry(
                            principal,
                            host,
                            AclOperation.WRITE,
                            AclPermissionType.ALLOW
                    )
            ),
            AclBinding(
                    resourcePattern,
                    AccessControlEntry(
                            principal,
                            host,
                            AclOperation.CREATE,
                            AclPermissionType.ALLOW
                    )
            )
    )
}
fun createConsumerACL(topic: String, user: String): List<AclBinding> {

    val topicPattern = ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
    val groupPattern = ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL)
    val principal = "User:$user"
    val host = "*"

    return listOf(
            AclBinding(
                    topicPattern,
                    AccessControlEntry(
                            principal,
                            host,
                            AclOperation.DESCRIBE,
                            AclPermissionType.ALLOW
                    )
            ),
            AclBinding(
                    topicPattern,
                    AccessControlEntry(
                            principal,
                            host,
                            AclOperation.READ,
                            AclPermissionType.ALLOW
                    )
            ),
            AclBinding(
                    groupPattern,
                    AccessControlEntry(
                            principal,
                            host,
                            AclOperation.READ,
                            AclPermissionType.ALLOW
                    )
            )
    )
}

fun kafkaProduce(brokersURL: String, topic: String, user: String, pwd: String, data: List<String>): Boolean =
    try {
        KafkaProducer<String, String>(
                Properties().apply {
                    set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                    set(ProducerConfig.CLIENT_ID_CONFIG, "funKafkaProduce")
                    set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                    set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                    set(ProducerConfig.ACKS_CONFIG, "all")
                    set(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
                    set(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500)
                    set("security.protocol", "SASL_PLAINTEXT")
                    set("sasl.mechanism", "PLAIN")
                    set("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                            "username=\"$user\" password=\"$pwd\";")
                }
        )
                .use { p ->
                    data.forEach { e ->
                        val info = p.send(ProducerRecord(topic, "", e)).get()
                        println("Producer kafka event details: [${info.topic()},${info.partition()},${info.offset()}]")
                    }
                    true
                }
    } catch (e: Exception) {
        println("PRODUCE EXCEPTION")
        false }

fun kafkaConsume(brokersURL: String, topic: String, user: String, pwd: String, lastEvent: String): List<String> =
        try {
            KafkaConsumer<String, String>(
                    Properties().apply {
                        set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                        set(ConsumerConfig.CLIENT_ID_CONFIG, "funKafkaConsume")
                        set(ConsumerConfig.GROUP_ID_CONFIG, "funKafkaConsumeGrpID")
                        set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                        set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                        set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                        set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3)
                        set("security.protocol", "SASL_PLAINTEXT")
                        set("sasl.mechanism", "PLAIN")
                        set("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                "username=\"$user\" password=\"$pwd\";")
                    }
            )
                    .use { c ->
                        c.subscribe(listOf(topic))

                        val lOfEvents = mutableListOf<String>()

                        (1..20).asSequence()
                                .flatMap { _ ->
                                    c.poll(Duration.ofMillis(1_000))
                                            .map { e -> listOf(e.value().also { lOfEvents.add(it) }) }.asSequence()
                                }
                                .any { it == listOf(lastEvent) }

                        lOfEvents
                    }
        } catch (e: Exception) {
            println("CONSUME EXCEPTION")
            emptyList()
        }
