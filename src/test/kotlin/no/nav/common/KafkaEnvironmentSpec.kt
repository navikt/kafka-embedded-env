package no.nav.common

import com.github.kittinunf.fuel.httpGet
import kafka.utils.ZkUtils
import no.nav.common.embeddedkafkarest.KRServer
import no.nav.common.embeddedzookeeper.ZKServer
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.xit

object KafkaEnvironmentSpec : Spek({

    val sessTimeout = 1500
    val connTimeout = 500
    val srTopic = 1
    var urls: Map<String, String> = emptyMap()

    describe("active embeddedkafka env of one broker with none topics created") {

        val b = 1
        val t = emptyList<String>()

        beforeGroup {
            urls = KafkaEnvironment.start(b, t)
        }

        it("should have $b broker(s)") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nBroker = allBrokersInCluster.size()
                close()
                nBroker
            } `should be equal to` b

        }

        it("should only be schema reg topic available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nTopics = allTopics.size()
                close()
                nTopics
            } `should be equal to` (t.size + srTopic)
        }

        it("should have a schema reg url different from empty string") {

            urls["schema"] shouldNotEqual ""
        }

        it("should have a schema reg port different from 0") {

            urls["schema"]?.split(":")?.last()?.toInt() ?: 0 shouldNotEqual 0
        }

        afterGroup {
            KafkaEnvironment.stop()
        }
    }

    describe("active embeddedkafka env of 1 broker with topics created") {

        val b = 1
        val t = listOf("test1")

        beforeGroup {
            KafkaEnvironment.start(b, t)
        }

        it("should have $b broker(s)") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nBroker = allBrokersInCluster.size()
                close()
                nBroker
            } `should be equal to` b

        }

        it("should be ${t.size} + schema reg topics available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nTopics = allTopics.size()
                close()
                nTopics
            } `should be equal to` (t.size + srTopic)
        }

        it("should have topics as requested available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val topics = allTopics
                val lTopics = mutableListOf<String>()

                topics.foreach { lTopics.add(it) }
                close()
                lTopics
            } `should contain all` t
        }

        afterGroup {
            KafkaEnvironment.stop()
        }
    }

    describe("active embeddedkafka env of 2 brokers with topics created") {

        val b = 2
        val t = listOf("test1","test2","test3","test4")

        beforeGroup {
            KafkaEnvironment.start(b, t)
        }

        it("should have $b broker(s)") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nBroker = allBrokersInCluster.size()
                close()
                nBroker
            } `should be equal to` b

        }

        it("should be ${t.size} + schema reg topics available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val nTopics = allTopics.size()
                close()
                nTopics
            } `should be equal to` (t.size + srTopic)
        }

        it("should have topics as requested available") {

            ZkUtils.apply(ZKServer.getUrl(), sessTimeout, connTimeout, false).run {
                val topics = allTopics
                val lTopics = mutableListOf<String>()

                topics.foreach { lTopics.add(it) }
                close()
                lTopics
            } `should contain all` t
        }

        it("should have $b broker urls") {

            urls["broker"]?.split(",")?.size ?: 0 shouldEqualTo  2

        }

        xit("should report topics as requested via rest") {

            // quick and raw http&json

            (KRServer.getUrl() + "/topics")
                    .httpGet()
                    .responseString().third.component1() shouldEqual """["_schemas","test1","test2","test3","test4"]"""
        }

        xit("should report $b brokers via rest") {

            // quick and raw http&json

            (KRServer.getUrl() + "/brokers")
                    .httpGet()
                    .responseString().third.component1() shouldEqual """{"brokers":[0,1]}"""
        }

        afterGroup {
            KafkaEnvironment.stop()
        }
    }

})