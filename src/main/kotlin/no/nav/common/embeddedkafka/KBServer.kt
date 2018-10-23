package no.nav.common.embeddedkafka

import kafka.metrics.KafkaMetricsReporter
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.VerifiableProperties
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.ServerStatus
import no.nav.common.kafkaAdmin
import no.nav.common.kafkaClient
import org.apache.kafka.common.utils.Time
import scala.Option
import java.io.File
import java.util.Properties

class KBServer(
    override val port: Int,
    id: Int,
    noPartitions: Int,
    logDir: File,
    zkURL: String,
    minSecurity: Boolean
) : ServerBase() {

    // see link for KafkaServerStartable below for starting up an embedded kafka broker
    // https://github.com/apache/kafka/blob/2.0/core/src/main/scala/kafka/server/KafkaServerStartable.scala

    override val url = if (minSecurity) "SASL_PLAINTEXT://$host:$port" else "PLAINTEXT://$host:$port"

    private val broker = KafkaServer(
            KafkaConfig(getDefaultProps(id, zkURL, noPartitions, logDir, minSecurity)),
            Time.SYSTEM,
            Option.apply(""),
            KafkaMetricsReporter.startReporters(
                    VerifiableProperties(
                            getDefaultProps(id, zkURL, noPartitions, logDir, minSecurity)
                    )
            )
    )

    override fun start() = when (status) {
        ServerStatus.NotRunning -> {
            broker.startup()
            status = ServerStatus.Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        ServerStatus.Running -> {
            broker.shutdown()
            broker.awaitShutdown()
            status = ServerStatus.NotRunning
        }
        else -> {}
    }

    private fun getDefaultProps(
        id: Int,
        zkURL: String,
        noPartitions: Int,
        logDir: File,
        minSecurity: Boolean
    ) = Properties().apply {

        // see link below for details - trying to make lean embedded embeddedkafka broker
        // https://kafka.apache.org/documentation/#brokerconfigs

        if (minSecurity) {
            set("security.inter.broker.protocol", "SASL_PLAINTEXT")
            set("sasl.mechanism.inter.broker.protocol", "PLAIN")
            set("sasl.enabled.mechanisms", "PLAIN")
            // set("listener.name.sasl_plaintext.plain.sasl.server.callback.handler.class", "")
            set("authorizer.class.name", "kafka.security.auth.SimpleAclAuthorizer")
            // see JAASContext object
            set("super.users", "User:${kafkaAdmin.username};User:${kafkaClient.username}")
            // allow.everyone.if.no.acl.found=true
            // auto.create.topics.enable=false
            set("zookeeper.set.acl", "true")
        }

        set(KafkaConfig.ZkConnectProp(), zkURL)
        set(KafkaConfig.ZkConnectionTimeoutMsProp(), 500)
        set(KafkaConfig.ZkSessionTimeoutMsProp(), 30_000)

        set(KafkaConfig.BrokerIdProp(), id)
        set(KafkaConfig.ListenersProp(), url)

        set(KafkaConfig.NumNetworkThreadsProp(), 3) // 3
        set(KafkaConfig.NumIoThreadsProp(), 8) // 8
        set(KafkaConfig.BackgroundThreadsProp(), 10) // 10

        // noPartitions is identical with no of brokers
        set(KafkaConfig.NumPartitionsProp(), noPartitions)
        set(KafkaConfig.DefaultReplicationFactorProp(), noPartitions)
        set(KafkaConfig.MinInSyncReplicasProp(), 1)

        set(KafkaConfig.OffsetsTopicPartitionsProp(), noPartitions) // 50
        set(KafkaConfig.OffsetsTopicReplicationFactorProp(), noPartitions.toShort()) // 3

        set(KafkaConfig.TransactionsTopicPartitionsProp(), noPartitions) // 50
        set(KafkaConfig.TransactionsTopicReplicationFactorProp(), noPartitions.toShort()) // 3
        set(KafkaConfig.TransactionsTopicMinISRProp(), noPartitions)

        // set(KafkaConfig.RequestTimeoutMsProp(), 2_000)
        // set(KafkaConfig.ReplicaSocketTimeoutMsProp(), 2_000)

        set(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp(), 10)

        // set("log.dir", "/tmp/kafka-logs")
        set(KafkaConfig.LogDirsProp(), logDir.absolutePath)

        set(KafkaConfig.AutoCreateTopicsEnableProp(), true.toString())

        set(KafkaConfig.NumRecoveryThreadsPerDataDirProp(), 1)

        set(KafkaConfig.ControlledShutdownMaxRetriesProp(), 1)
        set(KafkaConfig.ControlledShutdownRetryBackoffMsProp(), 500)
    }
}