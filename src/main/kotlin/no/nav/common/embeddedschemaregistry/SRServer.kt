package no.nav.common.embeddedschemaregistry

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.ServerStatus
import java.util.Properties

class SRServer(override val port: Int, private val kbURL: String, private val withSecurity: Boolean = false) : ServerBase() {

    // see link below for starting up embeddedschemaregistry
    // https://github.com/confluentinc/schema-registry/blob/5.0.x/core/src/main/java/io/confluent/kafka/schemaregistry/rest/SchemaRegistryMain.java

    override val url = "http://$host:$port"

    // not possible to stop and restart schema registry at this level, use inner core class
    private class SRS(url: String, kbURL: String, withSecurity: Boolean) {

        val scServer = SchemaRegistryRestApplication(
                SchemaRegistryConfig(
                        Properties().apply {
                            if (withSecurity) {
                                set(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                                set(SchemaRegistryConfig.KAFKASTORE_SASL_MECHANISM_CONFIG, "PLAIN")
                            }
                            set(SchemaRegistryConfig.LISTENERS_CONFIG, url)
                            set(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, kbURL)
                            set(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas")
                        }
                )
        )
    }

    private val sr = mutableListOf<SRS>()

    override fun start() = when (status) {
        ServerStatus.NotRunning -> {
            SRS(url, kbURL, withSecurity).apply {
                sr.add(this)
                scServer.start()
            }

            status = ServerStatus.Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        ServerStatus.Running -> {
            sr.first().apply {
                scServer.stop()
                scServer.join()
            }
            sr.removeAll { true }

            status = ServerStatus.NotRunning
        }
        else -> {}
    }
}