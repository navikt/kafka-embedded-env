package no.nav.common.kafka

import kafka.metrics.KafkaMetricsReporter
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.VerifiableProperties
import no.nav.common.utils.*
import no.nav.common.zookeeper.ZKServer
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.utils.Time
import scala.Option
import java.io.File
import java.util.*

class KBServer private constructor(override val port: Int, id: Int, private val noPartitions: Int) : ServerBase() {

    override val url = "PLAINTEXT://$host:$port"

    private val logDir = File(System.getProperty("java.io.tmpdir"),"inmkafkabroker/ID$id").apply {
        // in case of fatal failure and no deletion in previous run
        FileUtils.deleteDirectory(this)
    }

/*    private var RUNNING_AS_BROKER = BrokerState().apply {
        newState(3)
    }*/

    private val broker = KafkaServer(
            KafkaConfig(getDefaultProps(id)),
            Time.SYSTEM,
            Option.apply(""),
            KafkaMetricsReporter.startReporters(VerifiableProperties(getDefaultProps(id)))
    )

    override fun start() = broker.startup()

    override fun stop() {
        broker.shutdown()
        broker.awaitShutdown()
        FileUtils.deleteDirectory(logDir)
    }

    private fun getDefaultProps(id: Int) = Properties().apply {

        set(KafkaConfig.ZkConnectProp(), ZKServer.getUrl())
        set(KafkaConfig.ZkConnectionTimeoutMsProp(), 10_000)

        set(KafkaConfig.BrokerIdProp(),id)
        set(KafkaConfig.ListenersProp(), url)

        set(KafkaConfig.NumNetworkThreadsProp(),2)
        set(KafkaConfig.NumIoThreadsProp(),4)

        // log.dir is showing up in log output, but cannot find a corresponding kafka config option
        set("log.dir",logDir.absolutePath)
        set(KafkaConfig.LogDirsProp(),logDir.absolutePath)

        set(KafkaConfig.AutoCreateTopicsEnableProp(),true.toString())

        set(KafkaConfig.NumPartitionsProp(),noPartitions.toString())
        set(KafkaConfig.DefaultReplicationFactorProp(),1.toString())

        set(KafkaConfig.OffsetsTopicReplicationFactorProp(),1.toString())
        set(KafkaConfig.TransactionsTopicMinISRProp(),1.toString())
        set(KafkaConfig.TransactionsTopicReplicationFactorProp(),1.toString())
        set(KafkaConfig.NumRecoveryThreadsPerDataDirProp(),1.toString())
    }

    companion object : ServerActor<KBServer>() {

        //const val noOfBrokers = 2

        override fun onReceive(msg: ServerMessages) {

            when (msg) {
                is KBStart -> if (servers.isEmpty()) {

                    (0 until msg.noOfBrokers).forEach {
                        KBServer(getAvailablePort(),it, msg.noOfBrokers).run {
                            servers.add(this)
                            start()
                        }
                    }
                }

                KBStop -> if (!servers.isEmpty()) {

                    servers.asReversed().forEach { it.stop() }
                    servers.removeAll { true }
                }

                else -> {
                    // don't care about other messages
                }
            }
        }

        // really not relevant
        override fun getHost(): String = servers.firstOrNull()?.host ?: ""

        // really not relevant
        override fun getPort(): Int = servers.firstOrNull()?.port ?: 0

        // relevant for kafka rest
        override fun getUrl(): String = if (servers.isEmpty()) "" else
            servers.map { it.url }.foldRight("",{ u, acc -> if (acc.isEmpty()) u else "$u,$acc" })

    }

}