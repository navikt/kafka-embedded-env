package no.nav.common.embeddedkafka

import kafka.metrics.KafkaMetricsReporter
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.VerifiableProperties
import no.nav.common.embeddedutils.*
import org.apache.kafka.common.utils.Time
import scala.Option
import java.io.File
import java.util.*

class KBServer(
        override val port: Int,
        id: Int,
        noPartitions: Int,
        logDir: File,
        zkURL: String) : ServerBase() {

    // see link below for starting up a embeddedkafka broker
    // https://insight.io/github.com/apache/kafka/blob/1.0/core/src/main/scala/kafka/server/KafkaServerStartable.scala

    override val url = "PLAINTEXT://$host:$port"

    private val broker = KafkaServer(
            KafkaConfig(getDefaultProps(id, zkURL, noPartitions, logDir)),
            Time.SYSTEM,
            Option.apply(""),
            KafkaMetricsReporter.startReporters(
                    VerifiableProperties(
                            getDefaultProps(id, zkURL, noPartitions, logDir)
                    )
            )
    )

    override fun start() = when (status) {
        NotRunning -> {
            broker.startup()
            status = Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        Running -> {
            broker.shutdown()
            broker.awaitShutdown()
            status = NotRunning
        }
        else -> {}
    }

    private fun getDefaultProps(
            id: Int,
            zkURL: String,
            noPartitions: Int,
            logDir: File) = Properties().apply {

        // see link below for details - trying to make lean embedded embeddedkafka broker
        // https://kafka.apache.org/documentation/#brokerconfigs

        set(KafkaConfig.ZkConnectProp(), zkURL)
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
        set(KafkaConfig.MinInSyncReplicasProp(), 1)

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
}