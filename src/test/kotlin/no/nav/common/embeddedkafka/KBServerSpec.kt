package no.nav.common.embeddedkafka

import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldEqualTo
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object KBServerSpec : Spek({

    val kEnvKBSS = KafkaEnvironment(2)

    describe("kafka broker tests") {

        context("active embeddedkafka cluster of two broker") {

            val b = 2

            beforeGroup {
                kEnvKBSS.start()
            }

            it("should have $b broker(s)") {

                kEnvKBSS.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo b
            }

            it("should not be any topics available") {

                kEnvKBSS.adminClient.listTopics().names().get().toList().size shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        context("active embeddedkafka cluster of 1 stopped broker") {

            val b = 1

            beforeGroup {
                kEnvKBSS.serverPark.brokers[0].stop()
            }

            it("should have $b broker(s)") {

                kEnvKBSS.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo b
            }

            it("should not be any topics available") {

                kEnvKBSS.adminClient.listTopics().names().get().toList().size shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        context("active embeddedkafka cluster of 1 restarted broker") {

            val b = 2

            beforeGroup {
                kEnvKBSS.serverPark.brokers[0].start()
            }

            it("should have $b broker(s)") {

                kEnvKBSS.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo b
            }

            it("should not be any topics available") {

                kEnvKBSS.adminClient.listTopics().names().get().toList().size shouldEqualTo 0
            }

            afterGroup {
                // nothing here
            }
        }

        afterGroup {
            kEnvKBSS.tearDown()
        }
    }
})