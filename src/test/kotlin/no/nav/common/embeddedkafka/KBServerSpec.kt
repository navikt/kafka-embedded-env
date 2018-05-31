package no.nav.common.embeddedkafka

import kafka.utils.ZkUtils
import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object KBServerSpec : Spek({

    val kEnv = KafkaEnvironment(2)

    val sessTimeout = 1500
    val connTimeout = 500

    describe("kafka broker tests") {

        context("active embeddedkafka cluster of two broker") {

            val b = 2

            beforeGroup {
                kEnv.start()
            }

            it("should have $b broker(s)") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allBrokersInCluster.size()
                    close()
                    n
                } shouldEqualTo b
            }

            it("should not be any topics available") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allTopics.size()
                    close()
                    n
                } shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        context("active embeddedkafka cluster of 1 stopped broker") {

            val b = 1

            beforeGroup {
                kEnv.serverPark.brokers[0].stop()
            }

            it("should have $b broker(s)") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allBrokersInCluster.size()
                    close()
                    n
                } shouldEqualTo b
            }

            it("should not be any topics available") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allTopics.size()
                    close()
                    n
                } shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        context("active embeddedkafka cluster of 1 restarted broker") {

            val b = 2

            beforeGroup {
                kEnv.serverPark.brokers[0].start()
            }

            it("should have $b broker(s)") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allBrokersInCluster.size()
                    close()
                    n
                } shouldEqualTo b
            }

            it("should not be any topics available") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allTopics.size()
                    close()
                    n
                } shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        context("active embeddedkafka cluster of 2 stopped brokers") {

            val b = 0

            beforeGroup {
                kEnv.serverPark.brokers.forEach { it.stop() }
            }

            it("should have $b broker(s)") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allBrokersInCluster.size()
                    close()
                    n
                } shouldEqualTo b
            }

            it("should not be any topics available") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allTopics.size()
                    close()
                    n
                } shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        context("active embeddedkafka cluster of 2 restarted brokers") {

            val b = 2

            beforeGroup {
                kEnv.serverPark.brokers.forEach { it.start() }
            }

            it("should have $b broker(s)") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allBrokersInCluster.size()
                    close()
                    n
                } shouldEqualTo b
            }

            it("should not be any topics available") {

                ZkUtils.apply(kEnv.serverPark.zookeeper.url, sessTimeout, connTimeout, false).run {
                    val n = allTopics.size()
                    close()
                    n
                } shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})