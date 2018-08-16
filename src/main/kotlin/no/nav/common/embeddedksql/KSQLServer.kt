package no.nav.common.embeddedksql

import io.confluent.ksql.rest.server.Executable
import io.confluent.ksql.rest.server.KsqlRestApplication
import io.confluent.ksql.rest.server.KsqlRestConfig
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent
import no.nav.common.embeddedutils.NotRunning
import no.nav.common.embeddedutils.Running
import no.nav.common.embeddedutils.ServerBase
import org.apache.kafka.streams.StreamsConfig
import java.util.Properties

class KSQLServer(
    override val port: Int,
    private val kbURL: String,
    private val ksqlDir: String
) : ServerBase() {

    // see link below for starting up embeddedksql
    // https://github.com/confluentinc/ksql/blob/5.0.x/ksql-rest-app/src/test/java/io/confluent/ksql/rest/server/KsqlServerMainTest.java

    override val url = "http://$host:$port"

    // StandAloneExecutor is dependent on running kafka broker, want to connect with AdminClient
    private class KSS(url: String, kbURL: String, ksqlDir: String) {

        val restConfig = KsqlRestConfig(
                Properties().apply {
                    set("bootstrap.servers", kbURL)
                    set("listeners", url)
                    set("ksql.server.install.dir", ksqlDir)
                    set(StreamsConfig.APPLICATION_ID_CONFIG, "KSQL_REST_SERVER_DEFAULT_APP_ID")
                }
        )

        val executable: Executable = KsqlRestApplication.buildApplication(restConfig, KsqlVersionCheckerAgent())
    }

    private val ml = mutableListOf<KSS>()

    override fun start() = when (status) {
        NotRunning -> {
            KSS(url, kbURL, ksqlDir).apply {
                ml.add(this)
                executable.start()
            }
            status = Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        Running -> {
            ml.first().apply {
                executable.stop()
                executable.join()
            }
            ml.removeAll { true }

            status = NotRunning
        }
        else -> {}
    }
}