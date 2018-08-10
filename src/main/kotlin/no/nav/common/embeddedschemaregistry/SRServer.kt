package no.nav.common.embeddedschemaregistry

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.NotRunning
import no.nav.common.embeddedutils.Running
import java.util.Properties

class SRServer(override val port: Int, private val zkURL: String, private val kbURL: String) : ServerBase() {

    // see link below for starting up embeddedschemaregistry
    // https://github.com/confluentinc/schema-registry/blob/5.0.x/core/src/main/java/io/confluent/kafka/schemaregistry/rest/SchemaRegistryMain.java

    override val url = "http://$host:$port"

    // not possible to stop and restart schema registry at this level, use inner core class
    private class SRS(url: String, zkURL: String, kbURL: String) {

        val scServer = SchemaRegistryRestApplication(
                SchemaRegistryConfig(
                        Properties().apply {
                            set(SchemaRegistryConfig.LISTENERS_CONFIG, url)
                            // set(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkURL)
                            set(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, kbURL)
                            set(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas")
                        }
                )
        )
    }

    private val sr = mutableListOf<SRS>()

    override fun start() = when (status) {
        NotRunning -> {
            SRS(url, zkURL, kbURL).apply {
                sr.add(this)
                scServer.start()
            }

            status = Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        Running -> {
            sr.first().apply {
                scServer.stop()
                scServer.join()
            }
            sr.removeAll { true }

            status = NotRunning
        }
        else -> {}
    }
}