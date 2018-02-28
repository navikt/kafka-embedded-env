package no.nav.common.embeddedkafkarest

import io.confluent.kafkarest.KafkaRestApplication
import io.confluent.kafkarest.KafkaRestConfig
import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedschemaregistry.SRServer
import no.nav.common.embeddedutils.*
import java.util.*

class KRServer private constructor(override val port: Int) : ServerBase() {

    // see link below for starting up embeddedkafkarest
    // https://github.com/confluentinc/kafka-rest/blob/4.0.x/src/main/java/io/confluent/kafkarest/KafkaRestMain.java

    override val url = "http://$host:$port"

    private val krServer = KafkaRestApplication(Properties().apply {
        set(KafkaRestConfig.LISTENERS_CONFIG,url)
        set(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG,KBServer.getUrl())
        set(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG, SRServer.getUrl())
        set(KafkaRestConfig.PRODUCER_THREADS_CONFIG, 3)
    })

    override fun start() {

        krServer.start()
    }

    override fun stop() {

        try {
            krServer.stop()
            krServer.join()
        }
        catch (e: Exception) {
            // messy - current kafka server stop() throws null pointer exception...
            // do nothing - end of lifecycle for this rest server anyway
        }
    }

    companion object : ServerActor<KRServer>() {

        override fun onReceive(msg: ServerMessages) {
            when (msg) {
                KRStart -> if (servers.isEmpty()) {
                    KRServer(getAvailablePort()).run {
                        servers.add(this)
                        start()
                    }
                }

                KRStop -> if (!servers.isEmpty()) {
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