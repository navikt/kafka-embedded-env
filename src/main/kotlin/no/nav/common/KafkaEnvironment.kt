package no.nav.common

import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.admin.TopicCommand
import kafka.utils.ZkUtils
import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedutils.KBStart
import no.nav.common.embeddedutils.KBStop
import no.nav.common.embeddedutils.ZKStart
import no.nav.common.embeddedutils.ZKStop
import no.nav.common.embeddedzookeeper.ZKServer
import java.util.*

object KafkaEnvironment {

    fun start(noOfBrokers: Int = 1, topics: List<String> = emptyList()) {

        ZKServer.onReceive(ZKStart)
        KBServer.onReceive(KBStart(noOfBrokers))

        if (!topics.isEmpty()) createTopics(topics, noOfBrokers)
    }

    fun stop() {

        KBServer.onReceive(KBStop)
        ZKServer.onReceive(ZKStop)
    }

    // see the following links for creating topic
    // https://insight.io/github.com/apache/kafka/blob/1.0/core/src/main/scala/kafka/admin/TopicCommand.scala
    // https://insight.io/github.com/apache/kafka/blob/1.0/core/src/main/scala/kafka/utils/ZkUtils.scala
    // https://insight.io/github.com/apache/kafka/blob/1.0/core/src/main/scala/kafka/admin/AdminUtils.scala

    private fun createTopics(topics: List<String>, noPartitions: Int) {

        val sessTimeout = 1500
        val connTimeout = 500

        val zkUtils =  ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false)

        topics.forEach {

            // core/admin/TopicCommand for details
            val opts = TopicCommand.TopicCommandOptions(
                    arrayOf(
                            "--create",
                            it,
                            "--if-not-exists",
                            "--partitions",noPartitions.toString(),
                            "--replication-factor",1.toString(),
                            "--zookeeper",ZKServer.getUrl()
                    )
            )
            val config = Properties() // no advanced config of topic...
            val partitions = opts.options().valueOf(opts.partitionsOpt()).toInt()
            val replicas = opts.options().valueOf(opts.replicationFactorOpt()).toInt()
            val rackAwareDisabled = RackAwareMode.`Disabled$`()

            AdminUtils.createTopic(zkUtils, it, partitions, replicas, config, rackAwareDisabled)

            //while (!AdminUtils.topicExists(zkUtils, it)) {} //Thread.sleep(250) //waiting...
        }

        zkUtils.close()
    }

}