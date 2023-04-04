package no.nav.common.test.common

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.url
import io.ktor.http.ContentType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import no.nav.common.JAAS_PLAIN_LOGIN
import no.nav.common.JAAS_REQUIRED
import no.nav.common.embeddedutils.ServerBase
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.CommonClientConfigs
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
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import java.net.URL
import java.time.Duration
import java.util.Properties

// some kafka broker test utilities

data class TxtCmdRes(val txt: String, val cmd: (AdminClient?) -> Int, val res: Int)

fun AdminClient?.noOfBrokers(): Int = try {
    this?.describeCluster()?.nodes()?.get()?.toList()?.size ?: -1
} catch (e: Exception) {
    -1
}

val noOfBrokers: (AdminClient?) -> Int = { it.noOfBrokers() }

fun AdminClient?.noOfTopics(): Int = try {
    this?.listTopics()?.names()?.get()?.toList()?.size ?: -1
} catch (e: Exception) {
    -1
}

val noOfTopics: (AdminClient?) -> Int = { it.noOfTopics() }

fun AdminClient?.topics(): List<String> = try {
    this?.listTopics()?.names()?.get()?.toList() ?: emptyList()
} catch (e: Exception) {
    emptyList()
}

// some schema registry test utilities

const val SCHEMAREG_DefaultCompatibilityLevel = """{"compatibilityLevel":"BACKWARD"}"""
const val SCHEMAREG_NoSubjects = """[]"""

val scRegTests = mapOf(
    "should report default compatibility level" to Pair("/config", SCHEMAREG_DefaultCompatibilityLevel),
    "should report zero subjects" to Pair("/subjects", SCHEMAREG_NoSubjects),
)

suspend fun HttpClient.getSomething(endpoint: URL): String =
    this.get {
        url(endpoint)
        accept(ContentType.Application.Json)
    }.body()

val httpReqResp: (HttpClient, ServerBase, String) -> String = { client, sr, path ->
    try {
        val url = URL(sr.url + path)
        runBlocking {
            client.getSomething(url)
        }
    } catch (e: Exception) {
        e.javaClass.name
    }
}

// some kafka environment test utilities

fun createProducerACL(topicUser: Map<String, String>): List<AclBinding> =
    topicUser.flatMap {
        val (topic, user) = it

        listOf(AclOperation.DESCRIBE, AclOperation.WRITE, AclOperation.CREATE).let { lOp ->

            val tPattern = ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
            val principal = "User:$user"
            val host = "*"
            val allow = AclPermissionType.ALLOW

            lOp.map { op -> AclBinding(tPattern, AccessControlEntry(principal, host, op, allow)) }
        }
    }

fun createConsumerACL(topicUser: Map<String, String>): List<AclBinding> =
    topicUser.flatMap {
        val (topic, user) = it

        listOf(AclOperation.DESCRIBE, AclOperation.READ).let { lOp ->

            val tPattern = ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
            val gPattern = ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL)
            val principal = "User:$user"
            val host = "*"
            val allow = AclPermissionType.ALLOW

            lOp.map { op -> AclBinding(tPattern, AccessControlEntry(principal, host, op, allow)) } +
                AclBinding(gPattern, AccessControlEntry(principal, host, AclOperation.READ, allow))
        }
    }

suspend fun kafkaProduce(
    brokersURL: String,
    topic: String,
    user: String,
    pwd: String,
    data: Map<String, String>,
): Boolean =
    try {
        KafkaProducer<String, String>(
            Properties().apply {
                set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                set(ProducerConfig.CLIENT_ID_CONFIG, "funKafkaProduce")
                set(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer",
                )
                set(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer",
                )
                set(ProducerConfig.ACKS_CONFIG, "all")
                set(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
                set(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500)
                set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                set(SaslConfigs.SASL_MECHANISM, "PLAIN")
                set(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "$JAAS_PLAIN_LOGIN $JAAS_REQUIRED username=\"$user\" password=\"$pwd\";",
                )
            },
        ).use { p ->
            withTimeoutOrNull(10_000) {
                data.forEach { k, v -> p.send(ProducerRecord(topic, k, v)).get() }
                true
            } ?: false
        }
    } catch (e: Exception) {
        false
    }

suspend fun kafkaConsume(
    brokersURL: String,
    topic: String,
    user: String,
    pwd: String,
    noOfEvents: Int,
): Map<String, String> =
    try {
        KafkaConsumer<String, String>(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "funKafkaConsume")
                set(ConsumerConfig.GROUP_ID_CONFIG, "funKafkaConsumeGrpID")
                set(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer",
                )
                set(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer",
                )
                set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 4)
                set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                set(SaslConfigs.SASL_MECHANISM, "PLAIN")
                set(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "$JAAS_PLAIN_LOGIN $JAAS_REQUIRED username=\"$user\" password=\"$pwd\";",
                )
            },
        ).use { c ->
            c.subscribe(listOf(topic))

            val fE = mutableMapOf<String, String>()

            withTimeoutOrNull(10_000) {
                while (fE.size < noOfEvents) {
                    delay(100)
                    c.poll(Duration.ofMillis(500)).forEach { e -> fE[e.key()] = e.value() }
                }
                fE
            } ?: emptyMap()
        }
    } catch (e: Exception) {
        emptyMap()
    }

suspend fun kafkaAvroProduce(
    brokersURL: String,
    schemaRegistryUrl: String,
    topic: String,
    user: String,
    pwd: String,
    data: Map<String, GenericRecord>,
): Boolean =
    try {
        KafkaProducer<String, GenericRecord>(
            Properties().apply {
                set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                set(ProducerConfig.CLIENT_ID_CONFIG, "funKafkaAvroProduce")
                set(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer",
                )
                set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
                set(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                set(ProducerConfig.ACKS_CONFIG, "all")
                set(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
                set(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 500)
                set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                set(SaslConfigs.SASL_MECHANISM, "PLAIN")
                set(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "$JAAS_PLAIN_LOGIN $JAAS_REQUIRED username=\"$user\" password=\"$pwd\";",
                )
            },
        ).use { p ->
            withTimeoutOrNull(10_000) {
                data.forEach { k, v -> p.send(ProducerRecord(topic, k, v)).get() }
                true
            } ?: false
        }
    } catch (e: Exception) {
        false
    }

suspend fun kafkaAvroConsume(
    brokersURL: String,
    schemaRegistryUrl: String,
    topic: String,
    user: String,
    pwd: String,
    noOfEvents: Int,
): Map<String, String> =
    try {
        KafkaConsumer<String, String>(
            Properties().apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                set(ConsumerConfig.CLIENT_ID_CONFIG, "funKafkaAvroConsume")
                set(ConsumerConfig.GROUP_ID_CONFIG, "funKafkaAvroConsumeGrpID")
                set(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer",
                )
                set(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "io.confluent.kafka.serializers.KafkaAvroDeserializer",
                )
                set(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                set(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 4)
                set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                set(SaslConfigs.SASL_MECHANISM, "PLAIN")
                set(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "$JAAS_PLAIN_LOGIN $JAAS_REQUIRED username=\"$user\" password=\"$pwd\";",
                )
            },
        ).use { c ->
            c.subscribe(listOf(topic))

            val fE = mutableMapOf<String, String>()

            withTimeoutOrNull(10_000) {
                while (fE.size < noOfEvents) {
                    delay(100)
                    c.poll(Duration.ofSeconds(50)).forEach { e -> fE[e.key()] = e.value() }
                }
                fE
            } ?: emptyMap()
        }
    } catch (e: Exception) {
        throw RuntimeException(e)
    }
