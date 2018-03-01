package no.nav.common.embeddedschemaregistry

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication
import no.nav.common.embeddedutils.*
import no.nav.common.embeddedzookeeper.ZKServer
import java.util.*

class SRServer private constructor(override val port: Int) : ServerBase() {

    // see link below for starting up embeddedschemaregistry
    // https://github.com/confluentinc/schema-registry/blob/4.0.x/core/src/main/java/io/confluent/kafka/schemaregistry/rest/SchemaRegistryMain.java

    override val url = "http://$host:$port"

    private val scServer = SchemaRegistryRestApplication(
            Properties().apply {
                set(SchemaRegistryConfig.LISTENERS_CONFIG, url)
                set(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG,ZKServer.getUrl())
                set(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG,"_schemas")
            }
    )

    override fun start() = scServer.start()


    override fun stop() {
        scServer.stop()
        scServer.join()
    }

    companion object : ServerActor<SRServer>() {

        override fun onReceive(msg: ServerMessages) {

            when (msg) {
                SRStart -> if (servers.isEmpty()) {
                    SRServer(/*getAvailablePort()*/8081).run {
                        servers.add(this)
                        start()
                    }
                }

                SRStop -> if (!servers.isEmpty()) {
                    servers.first().stop()
                    servers.removeAt(0)
                }

                else -> {
                    // don't care about other messages
                }
            }

        }

        override fun getHost() = servers.firstOrNull()?.host ?: ""

        override fun getPort() = servers.firstOrNull()?.port ?: 0

        override fun getUrl() = servers.firstOrNull()?.url ?: ""
    }

}