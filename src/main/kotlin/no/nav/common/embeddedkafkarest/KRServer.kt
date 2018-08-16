package no.nav.common.embeddedkafkarest

import io.confluent.kafkarest.KafkaRestApplication
import io.confluent.kafkarest.KafkaRestConfig
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.NotRunning
import no.nav.common.embeddedutils.Running
import java.util.Properties

class KRServer(
    override val port: Int,
    private val kbURL: String,
    private var srURL: String
) : ServerBase() {

    // see link below for starting up embeddedkafkarest
    // https://github.com/confluentinc/kafka-rest/blob/5.0.x/kafka-rest/src/main/java/io/confluent/kafkarest/KafkaRestMain.java

    // TODO Kafka Rest have dependencies to kafka 1.1.0, both for kafka and kafka client...
    // see https://github.com/confluentinc/kafka-rest/blob/5.0.x/kafka-rest/pom.xml
    // Thus, this code is getting an exception - NoClassDefFoundError for kafka/common/InvalidConfigException
    // 2 options
    // 1) hacky shading
    // 2) ask confluent when they will upgrade Kafka Rest dependencies to kafka 2.x

    override val url = "http://$host:$port"

    // not possible to restart rest at this level, use inner core class
    private class KRS(url: String, kbURL: String, srURL: String) {

        val krServer: KafkaRestApplication = KafkaRestApplication(
                KafkaRestConfig(
                        Properties().apply {
                            // set(KafkaRestConfig.ID_CONFIG, "EMBREST")
                            set(KafkaRestConfig.LISTENERS_CONFIG, url)
                            set(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG, kbURL)
                            set(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, srURL)
                            set(KafkaRestConfig.PRODUCER_THREADS_CONFIG, 2) // 5
                        }
                )
        )
    }

    private val kr = mutableListOf<KRS>()

    override fun start() = when (status) {
        NotRunning -> {
            KRS(url, kbURL, srURL).apply {
                kr.add(this)
                try { krServer.start() } catch (e: Exception) { /* nothing*/ }
            }
            status = Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        Running -> {
            kr.first().apply {
                krServer.stop()
                krServer.join()
            }
            kr.removeAll { true }
            status = NotRunning
        }
        else -> {}
    }
}