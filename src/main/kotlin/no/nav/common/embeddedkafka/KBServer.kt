package no.nav.common.embeddedkafka

import kafka.metrics.KafkaMetricsReporter
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.VerifiableProperties
import no.nav.common.embeddedutils.*
import no.nav.common.embeddedzookeeper.ZKServer
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.utils.Time
import scala.Option
import java.io.File
import java.io.IOException
import java.util.*

class KBServer private constructor(override val port: Int, id: Int, private val noPartitions: Int) : ServerBase() {

    // see link below for starting up a embeddedkafka broker
    // https://insight.io/github.com/apache/kafka/blob/1.0/core/src/main/scala/kafka/server/KafkaServerStartable.scala

    override val url = "PLAINTEXT://$host:$port"

    //TODO - need to find a better solution in order to do proper cleanup
    private val logDir = File(
            System.getProperty("java.io.tmpdir"),"inmkafkabroker/ID$id${UUID.randomUUID()}").apply {
        // in case of fatal failure and no deletion in previous run
        try {
            FileUtils.deleteDirectory(this)
        }
        catch (e: IOException) {
            // Different behaviour between Mac and Windows
            // Catch whatever and do not care due to shutdown
        }
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
        try {
            broker.shutdown()
            broker.awaitShutdown()
            FileUtils.deleteDirectory(logDir)
        }
        catch(e: IOException) {
            // Different behaviour between Mac and Windows
            // Catch whatever and do not care due to shutdown
        }
    }

    private fun getDefaultProps(id: Int) = Properties().apply {

        // see link below for details - trying to make lean embedded embeddedkafka broker
        // https://kafka.apache.org/documentation/#brokerconfigs

        set(KafkaConfig.ZkConnectProp(), ZKServer.getUrl())
        set(KafkaConfig.ZkConnectionTimeoutMsProp(), 500)
        set(KafkaConfig.ZkSessionTimeoutMsProp(), 30_000)

        set(KafkaConfig.BrokerIdProp(),id)
        set(KafkaConfig.ListenersProp(), url)

        set(KafkaConfig.NumNetworkThreadsProp(),3) //3
        set(KafkaConfig.NumIoThreadsProp(),8) //8
        set(KafkaConfig.BackgroundThreadsProp(), 10) //10

        // noPartitions is identical with no of brokers
        set(KafkaConfig.NumPartitionsProp(),noPartitions)
        set(KafkaConfig.DefaultReplicationFactorProp(), noPartitions)
        set(KafkaConfig.MinInSyncReplicasProp(), noPartitions)

        set(KafkaConfig.OffsetsTopicPartitionsProp(), noPartitions) //50
        set(KafkaConfig.OffsetsTopicReplicationFactorProp(), noPartitions.toShort()) //3

        set(KafkaConfig.TransactionsTopicPartitionsProp(), noPartitions) //50
        set(KafkaConfig.TransactionsTopicReplicationFactorProp(), noPartitions.toShort()) //3
        set(KafkaConfig.TransactionsTopicMinISRProp(), noPartitions)

        //set(KafkaConfig.RequestTimeoutMsProp(), 2_000)
        //set(KafkaConfig.ReplicaSocketTimeoutMsProp(), 2_000)

        set(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp(), 10)

        set("log.dir",logDir.absolutePath)
        set(KafkaConfig.LogDirsProp(),logDir.absolutePath)

        set(KafkaConfig.AutoCreateTopicsEnableProp(),true.toString())

        set(KafkaConfig.NumRecoveryThreadsPerDataDirProp(),1)

        set(KafkaConfig.ControlledShutdownMaxRetriesProp(), 1)
        set(KafkaConfig.ControlledShutdownRetryBackoffMsProp(), 500)
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

                    servers.forEach {
                        it.stop()
                    }
                    servers.removeAll { true }
                }

                else -> {
                    // don't care about other messages
                }
            }
        }

        // really not relevant - giving only for the eventual first server
        override fun getHost() = servers.firstOrNull()?.host ?: ""

        // really not relevant - as above
        override fun getPort() = servers.firstOrNull()?.port ?: 0

        // Returning string as "PLAINTEXT://<host>:<port>,PLAINTEXT://<host>:<port>,.." as no of brokers
        override fun getUrl() = if (servers.isEmpty()) "" else
            servers.map { it.url }.foldRight("",{ u, acc -> if (acc.isEmpty()) u else "$u,$acc" })

    }

}