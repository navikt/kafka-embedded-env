package no.nav.common.embeddedkafka

import kafka.utils.ZkUtils
import no.nav.common.embeddedutils.KBStart
import no.nav.common.embeddedutils.KBStop
import no.nav.common.embeddedutils.ZKStart
import no.nav.common.embeddedutils.ZKStop
import no.nav.common.embeddedzookeeper.ZKServer
import org.amshove.kluent.`should be equal to`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object KBServerSpec : Spek({

    val sessTimeout = 1500
    val connTimeout = 500

    describe("active embeddedkafka cluster of one broker (start/stop)") {

        val b = 1

        beforeGroup {

            ZKServer.onReceive(ZKStart)
            KBServer.onReceive(KBStart(b))
        }

        it("should have $b broker(s)") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nBroker = allBrokersInCluster.size()
                close()
                nBroker
            } `should be equal to` b

        }

        it("should not be any topics available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nTopics = allTopics.size()
                close()
                nTopics
            } `should be equal to` 0
        }

        afterGroup {

            KBServer.onReceive(KBStop)
            ZKServer.onReceive(ZKStop)
        }
    }

    describe("active embeddedkafka cluster of 2 brokers (start/stop) for 2nd time") {

        val b = 2

        beforeGroup {

            ZKServer.onReceive(ZKStart)
            KBServer.onReceive(KBStart(b))
        }

        it("should have $b broker(s)") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nBroker = allBrokersInCluster.size()
                close()
                nBroker
            } `should be equal to` b

        }

        it("should not be any topics available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nTopics = allTopics.size()
                close()
                nTopics
            } `should be equal to` 0
        }

        afterGroup {

            KBServer.onReceive(KBStop)
            ZKServer.onReceive(ZKStop)
        }
    }

    describe("inactive embeddedkafka cluster (no start/stop)") {

        beforeGroup {  }

        it("should return empty string as host") {
            KBServer.getHost() `should be equal to` ""
        }

        it("should return 0 as port") {
            KBServer.getPort() `should be equal to` 0
        }

        it("should return empty string as url") {
            KBServer.getUrl() `should be equal to` ""
        }

        afterGroup {  }
    }
})