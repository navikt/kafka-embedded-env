package no.nav.common.embeddedschemaregistry

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication
import no.nav.common.embeddedutils.*
import java.util.*

class SRServer(override val port: Int, private val zkURL: String) : ServerBase() {

    // see link below for starting up embeddedschemaregistry
    // https://github.com/confluentinc/schema-registry/blob/4.0.x/core/src/main/java/io/confluent/kafka/schemaregistry/rest/SchemaRegistryMain.java

    override val url = "http://$host:$port"

    // not possible to stop and restart schema registry at this level, use inner core class
    private class SRS(url: String, zkURL: String) {

        val scServer = SchemaRegistryRestApplication(
                Properties().apply {
                    set(SchemaRegistryConfig.LISTENERS_CONFIG, url)
                    set(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkURL)
                    set(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "_schemas")
                }
        )
    }

    private val sr = mutableListOf<SRS>()

    override fun start() = when (status) {
        NotRunning -> {
            SRS(url, zkURL).apply {
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