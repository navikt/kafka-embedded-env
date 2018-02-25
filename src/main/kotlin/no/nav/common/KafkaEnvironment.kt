package no.nav.common

import kafka.admin.AdminUtils
import kafka.admin.RackAwareMode
import kafka.admin.TopicCommand
import kafka.utils.ZkUtils
import no.nav.common.kafka.KBServer
import no.nav.common.utils.KBStart
import no.nav.common.utils.KBStop
import no.nav.common.utils.ZKStart
import no.nav.common.utils.ZKStop
import no.nav.common.zookeeper.ZKServer
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

    private fun createTopics(topics: List<String>, noPartitions: Int) {

        val zkUtils =  ZkUtils.apply(ZKServer.getUrl(), 3_000, 3_000, false)

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

            while (!AdminUtils.topicExists(zkUtils, it)) Thread.sleep(250) //waiting...
        }

        zkUtils.close()
    }

}